import argparse
import os
import shutil

import geopandas as gpd
import pandas as pd


def filter_magnitude(gdf, remove_max):
    """
    Remove points with magnitude = 'maximum' if the remove_max flag is True.
    """
    if remove_max:
        return gdf[gdf['magnitude'] != 'maximum']
    return gdf


def combine_parquet_files(file1, file2, output_file, remove_max=False):
    """
    Combine two parquet files and write to the output, with an option to remove points where magnitude = 'maximum'.
    """
    try:
        gdf1 = gpd.read_parquet(file1)
        gdf2 = gpd.read_parquet(file2)

        # Optionally filter out points with magnitude = 'maximum'
        gdf1 = filter_magnitude(gdf1, remove_max)
        gdf2 = filter_magnitude(gdf2, remove_max)

        # Combine the two GeoDataFrames (concatenate rows)
        combined_gdf = pd.concat([gdf1, gdf2], ignore_index=True)

        # Write the combined GeoDataFrame to a new parquet file
        combined_gdf.to_parquet(output_file)
        print(f"Combined file written: {output_file}")

    except Exception as e:
        print(f"Error combining {file1} and {file2}: {e}")


def copy_file(source_file, destination_file, remove_max=False):
    """
    Copy a file to the destination directory, with an option to remove points where magnitude = 'maximum'.
    """
    try:
        gdf = gpd.read_parquet(source_file)

        # Optionally filter out points with magnitude = 'maximum'
        gdf = filter_magnitude(gdf, remove_max)

        # Save the filtered or unfiltered file to the destination
        gdf.to_parquet(destination_file)
        print(f"Copied file: {source_file} -> {destination_file}")

    except Exception as e:
        print(f"Error copying {source_file}: {e}")


def process_directories(dir1, dir2, output_dir, remove_max=False):
    """
    Process two directories to find matching .parquet files, combine points, and output the result.
    """
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all parquet files from both directories
    dir1_files = {f for f in os.listdir(dir1) if f.endswith('.parquet')}
    dir2_files = {f for f in os.listdir(dir2) if f.endswith('.parquet')}

    # Find matching and unmatched files
    matching_files = dir1_files.intersection(dir2_files)
    dir1_only_files = dir1_files - dir2_files
    dir2_only_files = dir2_files - dir1_files

    # Process matching files (combine points)
    combine_count = 0
    dir1_count = 0
    dir2_count = 0
    print("Merging duplicate files between dir1 and dir2...")
    for filename in matching_files:
        print(str(filename))
        file1 = os.path.join(dir1, filename)
        file2 = os.path.join(dir2, filename)
        output_file = os.path.join(output_dir, filename)
        combine_parquet_files(file1, file2, output_file, remove_max)
        combine_count += 1

    print('Copying unique files from dir1 and dir2...')
    # Copy unmatched files from dir1
    for filename in dir1_only_files:
        source_file = os.path.join(dir1, filename)
        destination_file = os.path.join(output_dir, filename)
        copy_file(source_file, destination_file, remove_max)
        dir1_count += 1

    # Copy unmatched files from dir2
    for filename in dir2_only_files:
        source_file = os.path.join(dir2, filename)
        destination_file = os.path.join(output_dir, filename)
        copy_file(source_file, destination_file, remove_max)
        dir2_count += 1

    print('Summary of processing:')
    print(f'Total combined files: {combine_count}')
    print(f'Source A only: {dir1_count}')
    print(f'Source B only: {dir2_count}')
    if len(os.listdir(output_dir)) != (combine_count + dir1_count + dir2_count):
        print('Warning: discrepancy in the expected number of total files in output dir')
    else:
        print(f'Total files in output: {len(os.listdir(output_dir))}')


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Process directories for flood data.')
    parser.add_argument(
        '-a',
        '--nws_directory_path',
        type=str,
        required=True,
        help='Path to the directory containing the NWS parquet points.',
    )
    parser.add_argument(
        '-b',
        '--usgs_directory_path',
        type=str,
        required=True,
        help='Path to the directory containing the USGS parquet points.',
    )
    parser.add_argument(
        '-o',
        '--output_directory_path',
        type=str,
        required=True,
        help='Path to the directory where new output files will be saved.',
    )
    parser.add_argument('-d', '--delete', action='store_true', help='Remove points with magnitude="maximum".')

    args = parser.parse_args()
    nws_directory_path = args.nws_directory_path
    usgs_directory_path = args.usgs_directory_path
    output_directory_path = args.output_directory_path

    # Process parquet files with the option to remove "maximum" magnitude points
    process_directories(
        nws_directory_path, usgs_directory_path, output_directory_path, remove_max=args.delete
    )
