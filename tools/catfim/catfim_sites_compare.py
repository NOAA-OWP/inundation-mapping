#!/usr/bin/env python3

import argparse
import os
import re
import sys
import traceback
from datetime import datetime, timezone

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import CRS
from shapely import wkt
from shapely.geometry import MultiPolygon, Point, Polygon
from shapely.ops import cascaded_union, unary_union


pd.options.mode.chained_assignment = None  # default='warn'

HV_CRS = CRS('EPSG:3857')

# import utils.fim_logger as fl
# FLOG = fl.FIM_logger()  # the non mp version

'''
This tool compares two or more versions of the CatFIM output site CSV. (Rewritten as of 3/8/25.)
It produces a compiled CSV of all sites and a CSV for each version comparison.
It will auto overwrite output files already existing.

Inputs:
- space-delimited list of CatFIM output paths to compare (-p)
- output save path (-o)
- optional flag to keep only sites with status changes in the comparison tables (-k)
- optional flag to generate spatial difference maps and a points geopackage for the version comparisons (-g)

- Example usage:
    python /foss_fim/tools/catfim_sites_compare.py
    -p  '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/fim_4_4_0_0_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/ /data/catfim/fim_4_5_2_0_flow_based/'
    -o '/home/emily.deardorff/notebooks/' -k -g

Outputs:
- Number of outputs depends on how many CatFIM results are provided.
- For example, if 3 versions of flow-based CatFIM are provided, the following outputs will be created:
    - flow_based_compare_all_versions.csv
    - flow_based_<version_1>_vs_<version_2>.csv
    - flow_based_<version_2>_vs_<version_3>.csv

- If both flow- and stage-based CatFIM are provided, a separate "compare_all_versions" CSV will be created for each product.

- Output CSVs:
    - <product_id>_compare_all_versions.csv
        Columns:
            site_id, nws_data_wfo, nws_data_rfc, HUC8, name, states,
            <version_1>_site_processed, <version_1>_catfim_mapped, <version_1>_status,
            <version_2>_site_processed, <version_2>_catfim_mapped, <version_2>_status,
            <version_3>_site_processed, <version_3>_catfim_mapped, <version_3>_status

    - <product_id>_<version_1>_vs_<version_2>.csv
        Columns:
            site_id, Change, Change Description,
            <version_1>_status, <version_2>_status,
            nws_data_wfo, nws_data_rfc, HUC8, name, states

    Note: product_id refers to either 'flow_based' or 'stage_based'

- Output GPKGs (produced if option -g flag was used):
    - <product_id>_<version_1>_vs_<version_2>_lost_coverage.gpkg
        Columns:
            site_id, magnitude, geometry_before, Change, Change Description,
            <version_1>_status, <version_2>_status, nws_data_wfo, nws_data_rfc, HUC8, name, states
    - <product_id>_<version_1>_vs_<version_2>_gained_coverage.gpkg
        Columns:
            site_id, magnitude, geometry_after, Change, Change Description,
            <version_1>_status, <version_2>_status, nws_data_wfo, nws_data_rfc, HUC8, name, states
    - <product_id>_<version_1>_vs_<version_2>.gpkg
        Columns:
            site_id, Change, Change Description,
            <version_1>_status, <version_2>_status, nws_data_wfo, nws_data_rfc, HUC8, name, states

Change Descriptions:
- No Change (Has mapped CatFIM in both versions)
- No Change (Doesn't have mapped CatFIM in either version)
- No Change (Site is excluded in both versions)
- Added (Site where CatFIM was not processed previously but now has mapped CatFIM)
- Added (Site where  CatFIM was not mapped previously but now has mapped CatFIM)
- Removed (Previously had mapped CatFIM, now site isn't being processed)
- Removed (Previously had mapped CatFIM, now site isn't being mapped)
- Status Change (Previously unmapped, now excluded from  processing)

Potential Upgrades TODO:
- Could add a flag to save a log file.
- Could add a flag to save a summary of the differences.
- Could implement sorting by version order in the output CSV.

'''


# Function that compiles CatFIM sites based on an input path list
def compile_catfim_sites(sorted_path_list):
    '''
    Inputs:
    - sorted_path_list: a list of string with paths to CatFIM runs (which should already be sorted in to flow-based or stage-based)

    Outputs:
    - combined_sites_df
    - combined_sites_metadata_df
    - version_id_list

    '''

    print(f'Results to compile: {sorted_path_list}')

    version_id_list = []

    for path in sorted_path_list:

        mapping_path = os.path.join(path, 'mapping')

        # Get the CSV filename and check that it exists
        csv_path = None
        for filename in os.listdir(mapping_path):
            if filename.endswith('sites.csv'):
                csv_path = os.path.join(mapping_path, filename)

        if csv_path is None:
            print(f'WARNING: No CSV path found for input path {path}')
            continue

        # Read in site CSV
        sites_df = pd.read_csv(csv_path)

        # Reconcile site ID column name
        if 'ahps_lid' in sites_df.columns:
            sites_df['site_id'] = sites_df['ahps_lid']
        elif 'nws_lid' in sites_df.columns:
            sites_df['site_id'] = sites_df['nws_lid']
        else:
            print(f'WARNING: Did not find ahps_lid or nws_lid column in {csv_path}')
            continue

        # Make a sites dataframes with only the needed columns
        sites_df['site_processed'] = 'yes'

        # Status dataframe
        trimmed_sites_df = sites_df[['site_id', 'site_processed', 'mapped', 'status']]

        # Metadata dataframe
        trimmed_site_metadata_df = sites_df[
            ['site_id', 'nws_data_wfo', 'nws_data_rfc', 'HUC8', 'name', 'states', 'geometry']
        ]

        # Pad 7-digit HUCs with a leading zero
        def add_leading_zero(num):
            num_str = str(num)
            if len(num_str) == 7:
                return '0' + num_str
            return num_str

        trimmed_site_metadata_df['HUC8'] = trimmed_site_metadata_df['HUC8'].apply(add_leading_zero)

        # Extract version_id from the path
        match = version_id = re.search(r'(hand|fim)_(\d+_\d+_\d+_\d+)', path)
        if match:
            version_id = match.group(2)
        else:
            print(f'WARNING: Unable to extract version ID from {path}')
            continue

        version_id_list.append(version_id)

        # Rename status 'mapped' and 'status' columns to have the version_id
        trimmed_sites_df.rename(
            columns={
                'mapped': f'{version_id}_catfim_mapped',
                'status': f'{version_id}_status',
                'site_processed': f'{version_id}_site_processed',
            },
            inplace=True,
        )

        # Add site status df to output status df
        try:
            combined_sites_df
            # If 'combined_sites_df' exists, perform an outer join with 'trimmed_sites_df'
            combined_sites_df = pd.merge(combined_sites_df, trimmed_sites_df, how='outer', on='site_id')
        except NameError:
            # If 'combined_sites_df' doesn't exist, assign 'trimmed_sites_df' to it
            combined_sites_df = trimmed_sites_df

        # Add site metadata df to output metadata df
        try:
            # Check if combined_sites_metadata_df already exists
            combined_sites_metadata_df

            # If it does, filter rows that are not already in combined_sites_metadata_df based on 'site_id'
            new_metadata_rows = trimmed_site_metadata_df[
                ~trimmed_site_metadata_df['site_id'].isin(combined_sites_metadata_df['site_id'])
            ]

            # If the metadata table exists, add information ONLY from any sites that weren't included
            combined_sites_metadata_df = pd.concat(
                [combined_sites_metadata_df, new_metadata_rows], ignore_index=True
            )

        except NameError:
            # If 'combined_sites_metadata_df' doesn't exist, assign 'trimmed_site_metadata_df' to it
            combined_sites_metadata_df = trimmed_site_metadata_df
        # End path loop

    # Error out if duplicate version IDs are detected
    if len(version_id_list) != len(set(version_id_list)):
        sys.exit(
            f'ERROR: Duplicate version IDs detected in path list: {version_id_list}. Remove duplicates and re-run.'
        )

    # Loop through columns and fill in details for NA columns
    for col in combined_sites_df.columns:
        if 'site_processed' in col:
            # Fill with 'no' where the value is not 'yes'
            combined_sites_df[col] = combined_sites_df[col].apply(lambda x: 'yes' if x == 'yes' else 'no')

        elif 'status' in col:
            # Get the version ID from the 'status' column
            version_id = col.replace('_status', '')

            # Construct the corresponding 'site_processed' column name
            site_processed_col = f'{version_id}_site_processed'

            # Check where the 'status' is NaN and 'site_processed' is 'no'
            combined_sites_df[col] = combined_sites_df.apply(
                lambda row: (
                    f'Site not processed in {version_id}. See release notes.'
                    if pd.isna(row[col]) and row[site_processed_col] == 'no'
                    else row[col]
                ),
                axis=1,
            )
    # End column loop

    # Join the site metadata to the combined_sites_df
    combined_sites_df = pd.merge(combined_sites_metadata_df, combined_sites_df, how='right', on='site_id')

    return combined_sites_df, combined_sites_metadata_df, version_id_list


# Create version comparison dataframe
def make_version_comparison_tables(
    combined_sites_df,
    combined_sites_metadata_df,
    product_id,
    version_id_list,
    out_save_path,
    keep_differences_only,
):
    '''
    Inputs:
    - combined_sites_df (dataframe of compiled sites from compile_catfim_sites)
    - combined_sites_metadata_df (dataframe of metadata for the sites from compile_catfim_sites)
    - product_id (string)
    - version_id_list (list of strings)
    - out_save_path (string)
    - keep_differences_only (True or False)

    Outputs:
    - CSVs and GPKGs saved to the out_save_path

    '''

    # Put versions in order
    def version_key(version):
        return list(map(int, version.split('_')))

    sorted_versions = sorted(version_id_list, key=version_key)

    # Iterate through versions (minus the last one) to calculate the Change and Change_Description columns
    for i in range(len(sorted_versions) - 1):

        old_version_id = sorted_versions[i]
        new_version_id = sorted_versions[i + 1]

        comparison_id = f'{product_id}_{old_version_id}_vs_{new_version_id}'
        comparison_table_save_path = os.path.join(out_save_path, f'{comparison_id}_sites.csv')

        # Define column names as variables
        old_site_processed_col = f'{old_version_id}_site_processed'
        new_site_processed_col = f'{new_version_id}_site_processed'

        old_catfim_mapped_col = f'{old_version_id}_catfim_mapped'
        new_catfim_mapped_col = f'{new_version_id}_catfim_mapped'

        old_catfim_status_col = f'{old_version_id}_status'
        new_catfim_status_col = f'{new_version_id}_status'

        old_catfim_version_col = 'previous_catfim_version'
        new_catfim_version_col = 'new_catfim_version'

        # Create a subset table with just the versions to compare
        compare_sites_df = combined_sites_df[
            [
                'site_id',
                old_site_processed_col,
                old_catfim_mapped_col,
                old_catfim_status_col,
                new_site_processed_col,
                new_catfim_mapped_col,
                new_catfim_status_col,
            ]
        ]

        # Add the version columns to the compare_sites_df
        compare_sites_df[old_catfim_version_col] = old_version_id
        compare_sites_df[new_catfim_version_col] = new_version_id

        # Initialize new columns with default values
        change_col = 'Change'
        change_description_col = 'Change_Description'
        default_error_message = 'ERROR - Site was unable to be categorized, check status columns manually.'
        compare_sites_df[change_col] = 'ERROR'
        compare_sites_df[change_description_col] = default_error_message

        # Define conditions
        conditions = [
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'yes'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_site_processed_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'yes')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_catfim_mapped_col] == 'no')
            & (compare_sites_df[new_site_processed_col] == 'no'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_catfim_mapped_col] == 'no'),
            (compare_sites_df[old_site_processed_col] == 'no')
            & (compare_sites_df[new_site_processed_col] == 'no'),
        ]

        # Define corresponding choices
        choices_change = [
            'No Change',
            'No Change',
            'Added',
            'Added',
            'Removed',
            'Removed',
            'Status Change',
            'Status Change',
            'No Change',
        ]

        choices_change_description = [
            'No Change (Does not have mapped CatFIM in either version)',
            'No Change (Has mapped CatFIM in both versions)',
            'Added (Site where CatFIM was not processed previously but now has mapped CatFIM)',
            'Added (Site where CatFIM was not mapped previously but now has mapped CatFIM)',
            'Removed (Previously had mapped CatFIM, now site is not being processed)',
            'Removed (Previously had mapped CatFIM, now site is not being mapped)',
            'Status Change (Previously was unmapped, now excluded from processing)',
            'Status Change (Previously excluded, now included but unmapped)',
            'No Change (Site is excluded in both versions)',
        ]

        # Apply conditions
        compare_sites_df[change_col] = pd.Series(
            pd.Categorical(np.select(conditions, choices_change, default='ERROR'))
        )
        compare_sites_df[change_description_col] = pd.Series(
            pd.Categorical(np.select(conditions, choices_change_description, default=default_error_message))
        )

        # Reorder columns and exclude the unnecessary ones
        compare_sites_df = compare_sites_df[
            [
                'site_id',
                change_col,
                change_description_col,
                old_catfim_version_col,
                old_catfim_status_col,
                new_catfim_version_col,
                new_catfim_status_col,
            ]
        ]

        # Rename status columns
        compare_sites_df.rename(
            columns={
                old_catfim_status_col: 'previous_catfim_status',
                new_catfim_status_col: 'new_catfim_status',
            },
            inplace=True,
        )

        # Join the site metadata to the compare_sites_df
        compare_sites_df = pd.merge(compare_sites_df, combined_sites_metadata_df, how='left', on='site_id')

        if keep_differences_only == True:
            # Remove rows where the value in 'change_col' is 'No Change'
            compare_sites_df = compare_sites_df[compare_sites_df[change_col] != 'No Change']

        # Save outputs
        compare_sites_df.to_csv(comparison_table_save_path, index=False)

        print(f'\nSaved comparison table to {comparison_table_save_path}')


# Read CatFIM library, remove intervals, and convert it to a gdf
def read_format_catfim_library(catfim_library_filepath):
    '''
    Inputs:
    - catfim_library_filepath (string)

    Outputs:
    - library_gdf (GeoDataFrame)

    TODO: Do I want to put this in a Try statement?
    '''

    library_table = pd.read_csv(catfim_library_filepath)

    # Remove intervals
    library_table = library_table[library_table['interval_stage'].isna()]

    # Convert the geometry column from WKT format to geometries
    library_table['geometry'] = library_table['geometry'].apply(wkt.loads)

    # Create a GeoDataFrame from the DataFrame
    library_gdf = gpd.GeoDataFrame(library_table, geometry='geometry')
    library_gdf = library_gdf.set_crs(HV_CRS)

    return library_gdf


def remove_polygon_shards(input_gdf, id_col, mag_col, minimum_area_threshold):
    '''
    Inputs:
    - input_gdf (GeoDataFrame)
    - id_col (string): column name for the site ID
    - mag_col (string): column name for the magnitude
    - minimum_area_threshold (float): minimum area threshold in square meters

    Outputs:
    - cleaned_gdf (GeoDataFrame): cleaned GeoDataFrame with polygons larger than the threshold
    '''

    # Check initial vertex count (for debugging)
    # total_vertices = count_verticies(input_gdf)
    # input_gdf['vertex_count_pre_filters'] = total_vertices
    # print(f"verticy count before area threshold and simplify is {total_vertices}")

    # Turn multipolygon into multiple polygons
    cleaned_gdf = input_gdf.explode()

    # Calculate area and remove polygon segments smaller than threshold (sq m)
    cleaned_gdf['area'] = cleaned_gdf.area
    cleaned_gdf = cleaned_gdf[cleaned_gdf['area'] > minimum_area_threshold]

    if len(cleaned_gdf) == 0:
        return None
    
    cleaned_gdf.reset_index(inplace=True, drop=True)

    # Condense back into a multipolygon
    cleaned_gdf = cleaned_gdf.dissolve()

    # Simplify the multipolygon
    cleaned_gdf['geometry'] = cleaned_gdf['geometry'].simplify(tolerance=5)

    # Count verticies after simplification
    total_vertices = count_verticies(cleaned_gdf)
    cleaned_gdf['vertex_count'] = total_vertices

    # Add vertex count to the cleaned GeoDataFrame for debugging
    # cleaned_gdf['vertex_count_post_filters'] = total_vertices
    # print(f"Vertex count before area threshold and after simplify is {total_vertices}")

    # Remove area column
    cleaned_gdf.drop('area', axis=1, inplace=True)
    cleaned_gdf = cleaned_gdf.reset_index()

    return cleaned_gdf

# Counts the number of verticies in a one-line GDF. This is primarily a debugging tool.
def count_verticies(gdf):
    total_vertices = 0
    if len(gdf) != 1:
        raise Exception(
            "This function is really designed for just one return value, so sending in a"
            " gdf with more than one rec will not work.")

    # Testing for the word "MultiPolygon" first because testing for "Polygon"
    # returns all records (Polygons and MultiPolygons).

    # If the geometry is a MultiPolygon, iterate through each polygon
    if 'MultiPolygon' in gdf.iloc[0]['geometry'].geom_type:
        for polygon in gdf.iloc[0]['geometry'].geoms:
            total_vertices += len(polygon.exterior.coords) - 1
            for interior_ring in polygon.interiors:
                total_vertices += len(interior_ring.coords) - 1

    # Otherwise, assume it is a Polygon
    elif 'Polygon' in gdf.iloc[0]['geometry'].geom_type:
        polygon = gdf.iloc[0]['geometry']
        total_vertices += len(polygon.exterior.coords) - 1
        for interior_ring in polygon.interiors:
            total_vertices += len(interior_ring.coords) - 1

    else:
        raise Exception(
            "Unable to count vertices for GDF with geometry type " + gdf.iloc[0]['geometry'].geom_type)

    # print(f"Geom type is {gdf.geometry.geom_type}")  # DEBUG
    # print(f'Total vertices: {total_vertices}')  # DEBUG

    return total_vertices


# Pivot table so there's a column per magnitude
def pivot_and_join_percent_change(areal_comparison, input_table_df, value_column_name, column_suffix):
    # Hardcode column names for id and magnitude
    id_col, mag_col = 'ahps_lid', 'magnitude'

    areal_comparison_pivot_df = (
        areal_comparison[[id_col, mag_col, value_column_name]]
        .pivot(index=id_col, columns=mag_col, values=value_column_name)
        .reset_index()
    )

    areal_comparison_pivot_df = areal_comparison_pivot_df.rename(
        columns={
            'action': 'action' + column_suffix,
            'minor': 'minor' + column_suffix,
            'moderate': 'moderate' + column_suffix,
            'major': 'major' + column_suffix,
            'record': 'record' + column_suffix,
        }
    )

    # Join to comparison table
    joined_table_df = pd.merge(
        input_table_df, areal_comparison_pivot_df, left_on='site_id', right_on=id_col, how='left'
    )

    # Move geometry to the last column
    geometry = joined_table_df.pop('geometry')
    joined_table_df.insert(len(joined_table_df.columns), 'geometry', geometry)

    return joined_table_df


# Calculate difference between CatFIM libraries of subsequent versions
def generate_spatial_difference_maps(
    sorted_path_list, product_id, version_id_list, output_save_filepath, debug_mode
):
    '''
    Inputs:
    - sorted_path_list (list of strings)
    - product_id (string)
    - version_id_list (list of strings)
    - output_save_filepath (string)

    Outputs:
    - GPKGs saved to the output_save_filepath
    - CSVs saved to the output_save_filepath

    '''
    try:
        print(f'\nGenerating spatial difference maps for {product_id}.')

        library_path_list = []
        for path in sorted_path_list:

            # Get the CSV filename and check that it exists
            mapping_path = os.path.join(path, 'mapping')
            csv_path = None
            for filename in os.listdir(mapping_path):
                if filename.endswith('library.csv'):
                    csv_path = os.path.join(mapping_path, filename)

            if csv_path is None:
                print(f'WARNING: No library CSV path found for input path {path}')
                continue

            library_path_list.append(csv_path)

        # Put versions in order
        sorted_versions = sorted(version_id_list, key=lambda version: list(map(int, version.split('_'))))

        # Iterate through versions (minus the last one) to calculate site change
        for i in range(len(sorted_versions) - 1):

            old_version_id = sorted_versions[i]
            new_version_id = sorted_versions[i + 1]
            comparison_id = f'{product_id}_{old_version_id}_vs_{new_version_id}'

            # Get filepaths
            old_version_library_csv_path = next(
                (path for path in library_path_list if old_version_id in path), None
            )
            new_version_library_csv_path = next(
                (path for path in library_path_list if new_version_id in path), None
            )

            if old_version_library_csv_path is None or new_version_library_csv_path is None:
                print(f'WARNING: Skipping GPKG formation for {comparison_id}')
                continue

            print(f'\nCreating comparison geopackages for {comparison_id}.')

            # Generate save paths
            lost_coverage_gpkg_save_path = os.path.join(
                output_save_filepath, f'{comparison_id}_lost_coverage.gpkg'
            )
            gained_coverage_gpkg_save_path = os.path.join(
                output_save_filepath, f'{comparison_id}_gained_coverage.gpkg'
            )

            read_gpkg_start_time = datetime.now(timezone.utc)

            # Read both versions of CatFIM library CSVs, remove intervals, and convert to a gdf
            before_gdf = read_format_catfim_library(old_version_library_csv_path)
            after_gdf = read_format_catfim_library(new_version_library_csv_path)

            read_gpkg_end_time = datetime.now(timezone.utc)
            gpkg_time_duration = read_gpkg_end_time - read_gpkg_start_time
            print(
                f"Time elapsed while reading in the CatFIM libraries: {str(gpkg_time_duration).split('.')[0]}"
            )
            print()

            id_col, mag_col = 'ahps_lid', 'magnitude'

            # Initialize empty GeoDataFrames for removed and added geometries
            removed_geom = gpd.GeoDataFrame(
                columns=[id_col, mag_col, 'geometry', 'removed_area_percent'], crs=after_gdf.crs
            )
            added_geom = gpd.GeoDataFrame(
                columns=[id_col, mag_col, 'geometry', 'added_area_percent'], crs=after_gdf.crs
            )

            print(f'Comparing {len(before_gdf)} before geometries to {len(after_gdf)} after geometries.')

            # Make a dataframe with all the lids and magnitudes in after_gdf and before_gdf
            combined_lids_gdf = pd.concat(
                [
                    before_gdf[[id_col, mag_col]].drop_duplicates(),
                    after_gdf[[id_col, mag_col]].drop_duplicates(),
                ]
            )
            combined_lids_gdf = combined_lids_gdf.drop_duplicates()
            combined_lids_gdf = combined_lids_gdf.reset_index(drop=True)
            print(f'Found {len(combined_lids_gdf)} unique lid/magnitude combinations.')

            debug_iterations = 100

            for i, (lid, magnitude) in enumerate(
                combined_lids_gdf[[id_col, mag_col]].itertuples(index=False)
            ):

                # Debug mode: Only run 100 site/magnitude combinations
                if debug_mode == True:
                    if i >= debug_iterations:
                        print("Breaking early as in debug mode")
                        break

                # Normal mode: Print progress every 100 iterations
                else:
                    if i % 100 == 0:
                        print(f'Processed {i} of {len(combined_lids_gdf)} geometries.')

                # Filter polygons by 'ahps_lid' and 'magnitude'
                before_polygons = before_gdf[(before_gdf[id_col] == lid) & (before_gdf[mag_col] == magnitude)]
                after_polygons = after_gdf[(after_gdf[id_col] == lid) & (after_gdf[mag_col] == magnitude)]

                # If polygons exist in both, find the difference
                if not before_polygons.empty and not after_polygons.empty:

                    before_union = before_polygons.geometry.union_all()
                    after_union = after_polygons.geometry.union_all()

                    removed = before_union.difference(after_union)
                    added = after_union.difference(before_union)

                    if not removed.is_empty:

                        removed_gdf = gpd.GeoDataFrame(
                            {
                                id_col: [lid],
                                mag_col: [magnitude],
                                'geometry': [removed],
                                'removed_area_percent': 0.0,
                            },
                            crs=removed_geom.crs,
                        )

                        removed_gdf_cleaned = remove_polygon_shards(
                            removed_gdf, id_col, mag_col, minimum_area_threshold=3000
                        )

                        # If removed_gdf_cleaned is None, skip to the next iteration
                        if removed_gdf_cleaned is None:
                            continue
                        else:
                            # Calculate % of the current area that is new
                            removed_area_percent = round((removed_gdf.area / before_union.area) * 100, 2)
                            removed_gdf_cleaned["removed_area_percent"] = removed_area_percent

                            removed_geom = pd.concat([removed_geom, removed_gdf_cleaned])

                    if not added.is_empty:

                        added_gdf = gpd.GeoDataFrame(
                            {
                                id_col: [lid],
                                mag_col: [magnitude],
                                'geometry': [added],
                                'added_area_percent': 0.0,
                            },
                            crs=added_geom.crs,
                        )
                        added_gdf_cleaned = remove_polygon_shards(
                            added_gdf, id_col, mag_col, minimum_area_threshold=3000
                        )

                        # If added_gdf_cleaned is None, skip to the next iteration
                        if added_gdf_cleaned is None:
                            continue
                        else:
                            # Calculate % of the current area that is new
                            added_area_percent = round((added_gdf_cleaned.area / after_union.area) * 100, 2)
                            added_gdf_cleaned["added_area_percent"] = added_area_percent

                            added_geom = pd.concat([added_geom, added_gdf_cleaned])

            # Read in the comparison table so we can add the % changes and re-save it
            comparison_table_save_path = os.path.join(output_save_filepath, f'{comparison_id}_sites.csv')
            comparison_table_df = pd.read_csv(comparison_table_save_path)

        # Run for added geom
        comparison_table_df = pivot_and_join_percent_change(
            added_geom,
            comparison_table_df,
            value_column_name='added_area_percent',
            column_suffix='_gained_coverage_percent',
        )

        # Run for removed geom
        comparison_table_df = pivot_and_join_percent_change(
            removed_geom,
            comparison_table_df,
            value_column_name='removed_area_percent',
            column_suffix='_lost_coverage_percent',
        )

        # Remove aphs_lid_x and aphs_lid_y columns if they exist
        existing_cols = comparison_table_df.columns.intersection(['ahps_lid_x', 'ahps_lid_y'])
        comparison_table_df = comparison_table_df.drop(columns=existing_cols, axis=1)

        # Save table
        comparison_table_df.to_csv(comparison_table_save_path, index=False)
        print(f'\nSaved updated comparison table to {comparison_table_save_path}')

        # Convert the sites GDF to a GeoDataFrame
        comparison_table_df['geometry'] = comparison_table_df['geometry'].apply(wkt.loads)

        compare_sites_gdf = gpd.GeoDataFrame(comparison_table_df, geometry='geometry')
        compare_sites_gdf = compare_sites_gdf.set_crs(HV_CRS)

        # Save the sites GDF as a GeoPackage
        comparison_gpkg_save_path = comparison_table_save_path.replace('.csv', '.gpkg')
        compare_sites_gdf.to_file(comparison_gpkg_save_path, layer='points', driver='GPKG')

        print(f'Saved comparison site GPKG to {comparison_gpkg_save_path}')

        # Add back in the metadata columns
        removed_geom = removed_geom.merge(
            before_gdf[[id_col, mag_col, 'huc', 'name', 'WFO', 'rfc', 'state', 'county']],
            on=[id_col, mag_col],
            how='left',
        )
        added_geom = added_geom.merge(
            before_gdf[[id_col, mag_col, 'huc', 'name', 'WFO', 'rfc', 'state', 'county']],
            on=[id_col, mag_col],
            how='left',
        )

        # Set the CRS (technically redundent but for safety)
        added_geom = added_geom.set_crs(HV_CRS)
        removed_geom = removed_geom.set_crs(HV_CRS)

        # Save the added and removed geometries to GPKGs and CSVs
        if len(added_geom) == 0:
            print('No gained coverage detected, not saving a gained coverage GPKG.')
        else:
            # Move geometry to the last column
            added_geom = added_geom[[col for col in added_geom.columns if col != 'geometry'] + ['geometry']]

            added_geom.to_file(
                gained_coverage_gpkg_save_path, index=False, layer='gained_coverage', driver='GPKG'
                )
            print(f'Saved gained coverage GPKG to {gained_coverage_gpkg_save_path}')

            # Save the added geom data as a csv as well
            added_geom.to_csv(gained_coverage_gpkg_save_path.replace('.gpkg', '.csv'), index=False)
            print(f'Saved gained coverage CSV to {gained_coverage_gpkg_save_path.replace(".gpkg", ".csv")}')

        if len(removed_geom) == 0:
            print('No lost coverage detected, not saving a lost coverage GPKG.')
        else:
            # Move geometry to the last column
            removed_geom = removed_geom[
                [col for col in removed_geom.columns if col != 'geometry'] + ['geometry']
            ]

            removed_geom.to_file(
                lost_coverage_gpkg_save_path, index=False, layer='lost_coverage', driver='GPKG'
                )
            print(f'Saved lost coverage GPKG to {lost_coverage_gpkg_save_path}')

            # Save the removed geom data as a csv as well
            removed_geom.to_csv(lost_coverage_gpkg_save_path.replace('.gpkg', '.csv'), index=False)
            print(f'Saved lost coverage CSV to {lost_coverage_gpkg_save_path.replace(".gpkg", ".csv")}')

    except Exception as e:
        print('ERROR: Generate spatial difference maps failed.')
        print(traceback.format_exc())
        sys.exit(f'ERROR: {e}')


# Main function for catfim_site_tracking
def main(path_list, output_save_filepath, keep_differences_only, generate_geopackages, debug_mode):
    '''
    Inputs
    - path_list (space-delimited list)
    - output_save_filepath (string)
    - keep_differences_only (True or False)
    - generate_geopackages (True or False)
    - debug_mode (True or False)

    Outputs
    - CSVs and GPKGs saved to the output_save_filepath

    '''

    # Verify that output save path exists
    # if not os.path.exists(output_save_filepath):
    #     sys.exit(f'ERROR: Output save path does not exist: {output_save_filepath}.')
    if not os.path.exists(output_save_filepath):
        os.makedirs(output_save_filepath, exist_ok=True)

    # Start stopwatch
    overall_start_time = datetime.now(timezone.utc)
    dt_string = overall_start_time.strftime("%m/%d/%Y %H:%M:%S")

    # NOTE: Removed logging file for now, since this is such a simple script.
    # Can implement later if needed by replacing print with FLOG.lprint
    # # Set up logging system
    # log_file_name = f"compare_log_file_{overall_start_time.strftime('%Y_%m_%d__%H_%M_%S')}"
    # log_path = os.path.join(output_save_filepath, log_file_name)
    # FLOG.setup(log_path)

    print('================================')
    print(f'Start CatFIM site comparison - (UTC): {dt_string}')
    print()

    # Print input parameters
    print('Input parameters: ')
    if keep_differences_only == True:
        print('    -k flag used -- Keeping only sites with status changes in the comparison tables')
    if generate_geopackages == True:
        print(
            '    -g flag used -- Generating spatial difference maps and site GPKGs '
            ' (takes about 60 to 90 mins per comparison depending on server)'
        )
    if debug_mode == True: 
        print('    -d flag used -- Debug mode in use. Only processing 100 polygons.')
    print(f'    -o -- Output save path: {output_save_filepath}')

    # Separate path list into flow- and stage-based lists
    # Initialize empty lists for stage and flow
    stage_path_list, flow_path_list = [], []

    # Separate space-delimited list into list
    path_list = path_list.split()

    # Loop through the path_list and categorize the paths
    for path in path_list:

        if not os.path.exists(os.path.join(path, 'mapping')):  # Check that mapping folder exists
            print(f'WARNING: Missing mapping folder in path {path}')
            continue
        elif 'stage' in path:
            stage_path_list.append(path)
        elif 'flow' in path:
            flow_path_list.append(path)
        else:
            print(f'WARNING: Unable to process path that does not contain "stage" or "flow": {path}')

    # Error out if duplicate paths are detected
    if len(path_list) != len(set(path_list)):
        print(
            f'ERROR: Duplicate paths detected in path list: {path_list}. \nRemove duplicate paths and re-run.'
        )
        sys.exit()

    # Run site compilation for stage-based CatFIM
    if len(stage_path_list) != 0:
        print('\n--------- Compiling stage-based CatFIM sites ---------')
        product_id = 'stage_based'

        stage_based_combined_sites_df, combined_sites_metadata_df, version_id_list = compile_catfim_sites(
            stage_path_list
        )

        # Save stage-based outputs
        out_save_path = os.path.join(output_save_filepath, f'{product_id}_compare_all_versions.csv')
        stage_based_combined_sites_df.to_csv(out_save_path, index=False)
        print(f'\nCombined stage-based outputs saved to {out_save_path}')

        if len(stage_path_list) > 1:

            # Make and save version comparison tables
            make_version_comparison_tables(
                stage_based_combined_sites_df,
                combined_sites_metadata_df,
                product_id,
                version_id_list,
                output_save_filepath,
                keep_differences_only,
            )

            # Generate spatial difference maps for stage-based
            if generate_geopackages == True:
                generate_spatial_difference_maps(
                    stage_path_list, product_id, version_id_list, output_save_filepath, debug_mode
                )

        else:
            print(
                f'\nWARNING: Only one version provided for {product_id}. Skipping version comparison tables and GPKGs.'
            )

    # Run site compilation for flow-based CatFIM
    if len(flow_path_list) != 0:
        print('\n--------- Compiling flow-based CatFIM sites ---------')
        product_id = 'flow_based'

        flow_based_combined_sites_df, combined_sites_metadata_df, version_id_list = compile_catfim_sites(
            flow_path_list
        )

        # Save flow-based outputs
        out_save_path = os.path.join(output_save_filepath, f'{product_id}_compare_all_versions.csv')
        flow_based_combined_sites_df.to_csv(out_save_path, index=False)
        print(f'\nCombined flow-based outputs saved to {out_save_path}')

        if len(flow_path_list) > 1:

            # Make and save version comparison tables
            make_version_comparison_tables(
                flow_based_combined_sites_df,
                combined_sites_metadata_df,
                product_id,
                version_id_list,
                output_save_filepath,
                keep_differences_only,
            )

            # Generate spatial difference maps for flow-based
            if generate_geopackages == True:
                generate_spatial_difference_maps(
                    flow_path_list, product_id, version_id_list, output_save_filepath, debug_mode
                )

        else:
            print(
                f'\nWARNING: Only one version provided for {product_id}. Skipping version comparison tables and GPKGs.'
            )

    # Wrap up
    overall_end_time = datetime.now(timezone.utc)
    print('\n================================')
    dt_string = overall_end_time.strftime("%m/%d/%Y %H:%M:%S")
    print(f'End sites compare - (UTC): {dt_string}')

    # Calculate duration
    time_duration = overall_end_time - overall_start_time
    print(f"Duration: {str(time_duration).split('.')[0]}")

    return


if __name__ == '__main__':

    '''
    This tool compares multiple versions of the CatFIM output site CSV.
    It will auto overwrite output files already existing.

    Sample usage:
    python /foss_fim/tools/catfim/catfim_sites_compare.py
    -p  '/data/catfim/hand_4_5_11_1_stage_based/ /data/catfim/fim_4_5_2_11_stage_based/ /data/catfim/fim_4_4_0_0_stage_based/ /data/catfim/hand_4_5_11_1_flow_based/ /data/catfim/fim_4_5_2_11_flow_based/ /data/catfim/fim_4_5_2_0_flow_based/'
    -o '/home/emily.deardorff/notebooks/'
    -k
    -g
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run CatFIM sites comparison.')

    parser.add_argument(
        '-p',
        '--path-list',
        help='REQUIRED: Space-delimited list of CatFIM output paths from which to compile sites.',
        required=True,
    )

    parser.add_argument(
        '-o',
        '--output-save-filepath',
        help='REQUIRED: Path to where the results files will be saved.',
        required=True,
    )

    parser.add_argument(
        '-k',
        '--keep-differences-only',
        help='OPTIONAL: Option to keep only changed sites in the comparison files.',
        required=False,
        action="store_true",
    )

    parser.add_argument(
        '-g',
        '--generate-geopackages',
        help='OPTIONAL: Option to generate spatial difference maps and a points geopackage for the version comparisons.',
        required=False,
        action="store_true",
    )

    parser.add_argument(
        '-d',
        '--debug-mode',
        help='OPTIONAL: Debug mode. Will only iterate through 100 records while generating geopackages.',
        required=False,
        action="store_true",
    )

    args = vars(parser.parse_args())

    try:
        main(**args)

    except Exception:
        print(traceback.format_exc())
