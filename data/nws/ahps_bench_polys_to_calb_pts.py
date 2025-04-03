import argparse
import gc
import logging
import os
import re
import sys

import geopandas as gpd
import pandas as pd


def find_max_elevation(interpolated_flows_path, attributes_path, df_poly_search, data_source):
    """
    Finds the maximum elevation from a flows CSV file, associates it with attributes, and updates paths.

    Parameters:
    - interpolated_flows_path (str): Path to the CSV file containing interpolated flows data.
    - attributes_path (str): Path to the CSV file containing attributes data.
    - data_source: String to identify "nws" or "usgs" data source

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

        if df_find_max['elevation'] is None:
            logging.warning(f"Skipping {interpolated_flows_path} due to no valid max elevation.")
            return None, None, None, None, None

        # Skip specific lids due to issues
        # tarn7: FIM multipolygons for max extent cause millions of points (ignore)
        skip_lids = ['tarn7']
        if df_find_max['nws_lid'] in skip_lids:
            logging.warning(
                f"Skipping the max stage/flow process - " f"manual override for: {df_find_max['nws_lid']}."
            )
            return None, None, None, None, None

        # Filter the dataframe to corresponding lid to check for poly search override
        new_search_lids = [df_find_max['nws_lid']]
        df_poly_search_subset = df_poly_search[df_poly_search['lid'].isin(new_search_lids)]
        if df_poly_search_subset.empty:
            df_poly_search_subset = None

        # Assign specific values to certain columns
        df_find_max['magnitude'] = 'maximum'  # Set the magnitude to "maximum"

        # Assign null values to certain columns
        columns_to_nullify = [
            'magnitude_stage',
            'magnitude_elev_navd88',
            'grid_stage',
            'grid_elev_navd88',
            'grid_flow_cfs',
        ]
        for col in columns_to_nullify:
            df_find_max[col] = None

        # Update the path using the new function
        updated_path = update_path_search(df_find_max['path'], df_poly_search_subset, data_source)
        if updated_path is None:
            logging.warning(f"Unexpected issue identifying polygon path: {updated_path}")
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


def update_path_search(original_path, df_poly_search_subset, data_source):
    """
    Updates the given path string by replacing certain patterns and checking if the file exists.

    Parameters:
    - original_path (str): The original file path to update.

    Returns:
    - str or None: The updated path if it exists, otherwise None.
    """
    try:
        # Replace "ahps_inundation_libraries" with "nws"
        if data_source == 'nws':
            updated_path = original_path.replace("ahps_inundation_libraries", str(data_source))
        if data_source == 'usgs':
            updated_path = original_path.replace("USGS_FIM", 'AHPS' + os.sep + str(data_source))

        # Replace "depth_grids" (case-insensitive) with "polygons"
        updated_path = re.sub(r"depth_grids", "polygons", updated_path, flags=re.IGNORECASE)
        # Replace "custom" (case-insensitive) with "polygons"
        updated_path = re.sub(r"custom", "polygons", updated_path, flags=re.IGNORECASE)
        # Replace "Depth_grid" (case-insensitive) with "Polygons"
        updated_path = re.sub(r"Depth_grid", "Polygons", updated_path, flags=re.IGNORECASE)

        # Replace the .tif with .shp
        # updated_path = updated_path.replace(".tiff", ".shp").replace(".tif", ".shp")
        # Extract the directory path
        directory_path = os.path.dirname(updated_path)
        # Extract file name without the extension
        file_name = os.path.splitext(os.path.basename(updated_path))[0]
        # Check if the there is data in df_poly_search_subset (already filtered to lid)
        if df_poly_search_subset is not None:
            # Check if file_name is in the 'tif_file_search' column of df_poly_search_subset
            if file_name in df_poly_search_subset['tif_file_search'].values:
                orig_file_name = file_name
                # Assign new file name from 'new_poly_search' column corresponding to the 'tif_file_search' match
                file_name = df_poly_search_subset.loc[
                    df_poly_search_subset['tif_file_search'] == file_name, 'new_poly_search'
                ].values[0]
                logging.info(
                    f"Replacing poly name search using provided csv: {orig_file_name} --> {file_name}"
                )
        # Add the new extension (.shp)
        shp_file_name = f"{file_name}.shp"
        # Combine directory path and new file name
        updated_path = os.path.join(directory_path, shp_file_name)

        # Check if the updated path file exists
        if os.path.exists(updated_path):
            logging.info(f"File exists: {updated_path}")
            return updated_path
        elif os.path.exists(re.sub(r"_0.shp", ".shp", updated_path, flags=re.IGNORECASE)):
            updated_path = re.sub(r"_0.shp", ".shp", updated_path, flags=re.IGNORECASE)
            logging.info(f"File exists: {updated_path}")
            return updated_path
        elif os.path.exists(re.sub(r"elev_", "", updated_path, flags=re.IGNORECASE)):
            updated_path = re.sub(r"elev_", "", updated_path, flags=re.IGNORECASE)
            logging.info(f"File exists: {updated_path}")
            return updated_path
        elif os.path.exists(re.sub(r".shp", "_0.shp", updated_path, flags=re.IGNORECASE)):
            updated_path = re.sub(r".shp", "_0.shp", updated_path, flags=re.IGNORECASE)
            logging.info(f"File exists: {updated_path}")
            return updated_path
        else:
            # combined search with removing both "_0" and "elev_"
            updated_path_0 = re.sub(r"_0.shp", ".shp", updated_path, flags=re.IGNORECASE)
            updated_path_elev = re.sub(r"elev_", "", updated_path_0, flags=re.IGNORECASE)
            if os.path.exists(updated_path_elev):
                logging.info(f"File exists: {updated_path_elev}")
                return updated_path_elev
            else:
                shp_file_name = 'elev_' + shp_file_name
                updated_path = os.path.join(directory_path, shp_file_name)
                if os.path.exists(updated_path):
                    logging.info(f"File exists: {updated_path}")
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


def find_flood_magnitude_shapes(attributes_path, interpolated_flows_path, df_poly_search, data_source):
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
        if 'magnitude' not in df_attributes.columns or 'grid_stage' not in df_attributes.columns:
            logging.error(f"Required columns 'magnitude' or 'grid_stage' not found in {attributes_path}.")
            return {}

        if 'stage' not in df_flows.columns:
            logging.error(f"Required column 'stage' not found in {interpolated_flows_path}.")
            return {}

        # Filter rows with non-null and non-empty 'magnitude' values
        df_valid = df_attributes[
            df_attributes['magnitude'].notnull() & (df_attributes['magnitude'].astype(str).str.strip() != '')
        ]
        df_valid = df_attributes[
            df_attributes['grid_stage'].notnull()
            & (df_attributes['grid_stage'].astype(str).str.strip() != '')
        ]

        # Remove all ".tif" extensions from the 'grid_stage' column
        # df_valid['grid_stage'] = df_valid['grid_stage'].str.replace('.tiff', '', regex=False)
        # df_valid['grid_stage'] = df_valid['grid_stage'].str.replace('.tif', '', regex=False)

        # Ensure that 'grid_stage' and 'stage' columns are strings in both dataframes
        df_valid['grid_stage'] = df_valid['grid_stage'].astype(str)
        df_flows['stage'] = df_flows['stage'].astype(str)

        # Check for any NaN or empty values before merging and replace them with empty strings
        df_valid['grid_stage'].fillna('', inplace=True)
        df_flows['stage'].fillna('', inplace=True)

        if df_valid.empty:
            logging.warning(f"No valid entries found in the 'magnitude' column of {attributes_path}.")
            return {}

        # Join df_valid and df_flows using 'grid_stage' from df_valid and 'stage' from df_flows
        df_joined = df_valid.merge(df_flows, left_on='grid_stage', right_on='stage', how='inner')

        # After merging, inspect other columns for data type consistency
        for col in df_joined.columns:
            if df_joined[col].dtype == 'object':
                # Convert any object columns to string to avoid mixed type issues
                df_joined[col] = df_joined[col].astype(str)

        if df_joined.empty:
            logging.warning(f"No matching entries found between attributes and flows data: {attributes_path}")
            return {}

        # Filter the dataframe to corresponding lid to check for poly search override
        new_search_lids = df_joined['nws_lid'].tolist()
        df_poly_search_subset = df_poly_search[df_poly_search['lid'].isin(new_search_lids)]
        if df_poly_search_subset.empty:
            df_poly_search_subset = None

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
            updated_path = update_path_search(path, df_poly_search_subset, data_source)
            if updated_path is None:
                logging.warning(f"Unexpected issue finding polygon path: {path}")
                continue

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


def process_shapefile_to_points_with_sampling(
    shapefile_path, study_limit_file, levee_file, point_attributes, data_source, step=5
):
    """
    Processes a shapefile to extract edge vertices with sampling, applies buffer operations using other shapefiles,
    and converts returns the filtered dataframe.

    Parameters:
    - shapefile_path (str): Path to the input shapefile containing polygon or multipolygon geometries.
    - study_limit_file (str): Path to the study limit shapefile, which will be used for buffering.
    - levee_file (str): Path to the levee shapefile, which will also be used for buffering.
    - point_attributes (dict): Dictionary containing point attributes like 'nws_lid', 'flow', 'magnitude', etc.,
                               to be added to each extracted point.
    - step (int, optional): Sampling step for extracting vertices from polygon edges. Defaults to 5.

    Returns:
    - gpd.GeoDataFrame: A GeoDataFrame containing sampled points with associated attributes.
                            Returns None if an error occurs.

    The function performs the following steps:
    1. Reads the input shapefile using GeoPandas.
    2. Logs and checks the types of geometries present in the shapefile.
    3. If the 'nws_lid' is dissolve list, it dissolves multipolygons into single polygons.
    4. Extracts edge vertices from polygons and multipolygons using a specified sampling step.
    5. Creates a GeoDataFrame of the sampled points, transforms its CRS to EPSG:5070, and attaches point attributes.
    6. Buffers geometries from the provided study limit and levee shapefiles and removes points within these buffer areas.
    7. Returns the GeoDataFrame of sampled points with attached attributes.

    Any errors encountered during processing are logged.
    """
    try:
        try:
            gdf = gpd.read_file(shapefile_path)
            # Check if the CRS is not set
            if gdf.crs is None:
                logging.debug(f"CRS is missing for {shapefile_path} --> Setting CRS to EPSG:4326.")
                gdf.set_crs(epsg=4326, inplace=True)

        except Exception as e:
            logging.error(f"Failed to read shapefile {shapefile_path}: {e}")
            return

        # Log the types of geometries found
        logging.debug(f"Geometries found in {shapefile_path}: {gdf.geometry.type.unique()}")

        # Check if the shapefile contains valid polygons or multipolygons
        if gdf.empty or not any(gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])):
            logging.warning(f"No valid polygons found in {shapefile_path}.")
            return

        # Dissolve MultiPolygons into single Polygons if nws_lid is in list below
        # Polygon data is binned by depths for these?
        dissolve_locations = ['tarn7', 'pgvn7']
        if point_attributes.get('nws_lid') in dissolve_locations:
            # if 'MultiPolygon' in gdf.geometry.geom_type.unique():
            print('Dissolving multipolygons...')
            # Unify all geometries into a single MultiPolygon
            unified_geom = gdf.unary_union

            # Convert the unified geometry back to a GeoDataFrame
            gdf = gpd.GeoDataFrame(geometry=[unified_geom], crs=gdf.crs)

            # Break the unified geometry into individual Polygons if necessary
            gdf = gdf.explode(index_parts=True, ignore_index=True)

            logging.debug(f"Dissolving multipolygons: {shapefile_path}")

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
        point_attributes['submitter'] = data_source  # Set the data source attribute
        point_attributes['coll_time'] = '2024-09-04'  # Set the data source attribute
        point_attributes['layer'] = point_attributes['magnitude']  # Set the data source attribute

        # Rename 'huc' to 'HUC8' if it exists
        if 'huc' in point_attributes:
            point_attributes['HUC8'] = point_attributes.pop('huc')

        # Define the list of attributes to keep
        attributes_to_keep = [
            'nws_lid',
            'magnitude',
            'layer',
            'path',
            'name',
            'elevation',
            'stage',
            'flow',
            'flow_unit',
            'flow_source',
            'coll_time',
            'submitter',
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


def process_directory(root_dir, data_source, manual_search_override, output_dir):
    root_dir = os.path.join(root_dir, data_source)
    # Check if the directory exists, and if not, exit with an error code
    if not os.path.exists(root_dir) or not os.path.isdir(root_dir):
        print(f"Error: Directory {root_dir} does not exist.")
        sys.exit(1)
    if not os.path.exists(manual_search_override):
        print(f"Error: Directory {manual_search_override} does not exist.")
        sys.exit(1)

    output_dir = os.path.join(output_dir, data_source)
    # Create the directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    log_file = os.path.join(output_dir, "processing_log.log")
    logging.basicConfig(
        filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
    )

    df_poly_search = pd.read_csv(manual_search_override)

    logging.info(f"Starting process in directory: {root_dir}")
    accumulated_data = []  # List to accumulate nws_lid and magnitude data
    lid_dict = {}  # List to accumulate all available nws_lid
    # Loop through each first-level directory under root_dir
    for huc_dir in reversed(sorted(os.listdir(root_dir))):
        print(huc_dir)
        first_level_path = os.path.join(root_dir, huc_dir)
        if os.path.isdir(first_level_path):
            all_points = gpd.GeoDataFrame()  # Initialize an empty GeoDataFrame to accumulate points
            # Now walk through each subdirectory within the first-level directory
            for subdir, dirs, files in os.walk(first_level_path):
                for file in files:
                    if file.endswith('_flows.csv'):
                        interpolated_flows_path = os.path.join(subdir, file)
                        print(f"Processing {interpolated_flows_path}.")
                        logging.info(f"Processing {interpolated_flows_path}.")

                        # Find the corresponding _attributes.csv file
                        if data_source == 'nws':
                            attributes_file = file.replace('_interpolated_flows.csv', '_attributes.csv')
                        else:
                            attributes_file = file.replace('_flows.csv', '_attributes.csv')
                        attributes_path = os.path.join(subdir, attributes_file)

                        if not os.path.exists(attributes_path):
                            logging.warning(
                                f"Attributes file {attributes_file} not found for {file} in {subdir}."
                            )
                        else:
                            lid = str(file)[:5]
                            # Add LID and HUC8 to the dictionary
                            lid_dict[lid] = str(huc_dir)
                            max_elevation, path_value, study_limit_file, levee_file, point_attributes = (
                                find_max_elevation(
                                    interpolated_flows_path, attributes_path, df_poly_search, data_source
                                )
                            )

                            if max_elevation is not None:
                                if check_max_elevation(attributes_path, max_elevation):
                                    print(
                                        f"Path with max elevation greater than magnitude_elev_navd88: {path_value}"
                                    )

                                    # Process the shapefile and collect points
                                    gdf_points = process_shapefile_to_points_with_sampling(
                                        path_value,
                                        study_limit_file,
                                        levee_file,
                                        point_attributes,
                                        data_source,
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
                                            point_attributes['nws_lid']
                                            + ' --> '
                                            + point_attributes['magnitude']
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
                                        # Clear GeoDataFrame after each file to free memory
                                        del gdf_points
                                        gc.collect()
                                else:
                                    logging.info(
                                        f"Max elevation in {interpolated_flows_path} is not greater than max magnitude_elev_navd88 in {attributes_file}."
                                    )

                            # Find flood magnitude shapes and process them
                            print('Checking for flood category data files...')
                            flood_cat_paths = find_flood_magnitude_shapes(
                                attributes_path, interpolated_flows_path, df_poly_search, data_source
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
                                    gdf_points = process_shapefile_to_points_with_sampling(
                                        path_value,
                                        study_limit_file,
                                        levee_file,
                                        point_attributes,
                                        data_source,
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
                                        # Clear GeoDataFrame after each file to free memory
                                        del gdf_points
                                        gc.collect()
                            else:
                                logging.info(
                                    f"Did not find any flood category maps to process in {attributes_file} or could not find corresponding data in {attributes_file}."
                                )

            if not all_points.empty:
                # Force columns to consistent data types
                # Convert all object columns to string
                for col in all_points.select_dtypes(include=['object']).columns:
                    all_points[col] = all_points[col].astype(str)

                # If there are numeric columns that should be int or float, make sure they are correct
                for col in all_points.select_dtypes(include=['int', 'float']).columns:
                    all_points[col] = pd.to_numeric(all_points[col], errors='coerce')

                # Define the output Parquet file path using the 8-digit subdirectory name
                parquet_path = os.path.join(output_dir, f"{huc_dir}.parquet")

                # Export the accumulated points to a Parquet file
                all_points.to_parquet(parquet_path)
                print(parquet_path)
                logging.info(f"Points from all files in subdirectory {subdir} saved to {parquet_path}")
                # Clear GeoDataFrame after each file to free memory
                del all_points
                gc.collect()

    # After processing all directories, group the accumulated data by nws_lid and export to CSV
    if accumulated_data:
        df_accumulated = pd.DataFrame(accumulated_data)
        # Check if any LIDs in lid_dict are missing from grouped_df, and add them
        for lid, huc8 in lid_dict.items():
            if lid not in df_accumulated['nws_lid'].values:
                # Create a new DataFrame with the missing LID and HUC8
                new_row = pd.DataFrame({'nws_lid': [lid], 'HUC8': [huc8], 'magnitude': ['']})

                # Concatenate the new row to df_accumulated
                df_accumulated = pd.concat([df_accumulated, new_row], ignore_index=True)

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
        '-s',
        '--source',
        type=str,
        choices=['nws', 'usgs'],  # Restrict the options to "nws" or "usgs"
        help='Choose to process either "nws" or "usgs"',
    )
    parser.add_argument(
        '-f',
        '--manual_search_override',
        type=str,
        help='Path to csv file with list of lids and manual search overrides to handle file name discrepancies',
    )
    parser.add_argument(
        '-o',
        '--output_directory_path',
        type=str,
        help='Path to the directory where output files will be saved.',
    )

    args = parser.parse_args()
    root_directory_path = args.root_directory_path
    data_source = args.source
    manual_search_override = args.manual_search_override
    output_directory_path = args.output_directory_path

    # root_directory_path = r'B:\FIM_development\fim_assessment\FOSS\ahps_benchmark\5_5_2024\nws'
    # output_directory_path = r'C:\GID\FOSS_FIM\ahps_max_stage_flow_preprocess\calibration_points\5_5_2024'
    process_directory(root_directory_path, data_source, manual_search_override, output_directory_path)
