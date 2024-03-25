import os
import fnmatch
import geopandas as gpd
import pandas as pd
import argparse
from multiprocessing import Pool


def find_gpkg_files(directory, filename):
    gpkg_files = []
    print("Using: '" + str(filename) + "' to search for relevant files...")
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".gpkg") and fnmatch.fnmatch(file, f"{filename}*"):
                gpkg_files.append(os.path.join(root, file))
    print("Found " + str(len(gpkg_files)) + " files to concatenate")
    return gpkg_files

def concatenate_gpkg(files):
    concatenated_gdf = gpd.GeoDataFrame()
    for file in files:
        gdf = gpd.read_file(file)
        concatenated_gdf = gpd.GeoDataFrame(pd.concat([concatenated_gdf, gdf], ignore_index=True))
    return concatenated_gdf

def concatenate_gpkg_multiprocessing(directory, filename, num_processes=4):
    gpkg_files = find_gpkg_files(directory, filename)
    chunks = [gpkg_files[i::num_processes] for i in range(num_processes)]
    with Pool(num_processes) as pool:
        results = pool.map(concatenate_gpkg, chunks)
    print("Concatenating files together...")
    concatenated_gdf = gpd.GeoDataFrame(pd.concat(results, ignore_index=True))
    return concatenated_gdf

def main(directory, filename, output_file, num_processes):
    concatenated_gdf = concatenate_gpkg_multiprocessing(directory, filename, num_processes)
    concatenated_gdf.to_file(output_file, driver="GPKG")
    print(f"Concatenated GeoPackage saved as '{output_file}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate GeoPackage files with a specific filename in a directory.")
    parser.add_argument("-d","--directory", help="Path to the directory containing GeoPackage files")
    parser.add_argument("-n","--filename", help="Specific filename (with wildcard) to search for. Example: demDerived_reaches_split_filtered_addedAttributes_crosswalked_*")
    parser.add_argument("-o", "--output", default="data/temp/concatenated.gpkg", help="Output file name for the concatenated GeoPackage")
    parser.add_argument("-j", "--processes", type=int, default=10, help="Number of processes for multiprocessing")
    args = parser.parse_args()

    main(args.directory, args.filename, args.output, args.processes)