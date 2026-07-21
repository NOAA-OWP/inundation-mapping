#!/usr/bin/env python3

import argparse
import os

import geopandas as gpd
import pandas as pd

import tools.catfim.catfim_post_processing as cpp
import tools.catfim.catfim_shared_functions as csf


'''
This tool is being used to combine the outputs of two CatFIM runs (primary and secondary) into a single set of outputs.

The primary and secondary directories must have the same values for CATFIM_TYPE, FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP,
and SEARCH (all found in the runtime_args.env file of the directories).

The outputs are merged into new files in the primary folder with a label added to the filename.

'''


def merge_gpkgs(gdf1_path, gdf2_path, output_gpkg_path):

    # Get filename without extension for the layer name
    filename = os.path.splitext(os.path.basename(gdf1_path))[0]

    # Read the GeoPackages into GeoDataFrames
    gdf1 = gpd.read_file(gdf1_path)
    gdf2 = gpd.read_file(gdf2_path)

    # Ensure both GeoDataFrames have the same Coordinate Reference System (CRS)
    if gdf1.crs != gdf2.crs:
        print(f"CRSs differ. Reprojecting second GeoDataFrame to match the first's CRS: {gdf1.crs}")
        gdf2 = gdf2.to_crs(gdf1.crs)

    # Concatenate the GeoDataFrames and save output
    merged_gdf = pd.concat([gdf1, gdf2], ignore_index=True)
    merged_gdf.to_file(output_gpkg_path, driver="GPKG", layer=filename)
    print(f"Successfully merged GeoPackages into {output_gpkg_path} in layer {filename}")

    return


def merge_csvs(csv1_path, csv2_path, output_csv_path):
    df1 = pd.read_csv(csv1_path)
    df2 = pd.read_csv(csv2_path)
    merged_df = pd.concat([df1, df2], ignore_index=True)
    merged_df.to_csv(output_csv_path, index=False)
    print(f"Successfully merged CSVs into {output_csv_path}")

    return


def merge_geoparquets(parquet1_path, parquet2_path, output_parquet_path):
    df1 = pd.read_parquet(parquet1_path)
    df2 = pd.read_parquet(parquet2_path)
    merged_df = pd.concat([df1, df2], ignore_index=True)
    merged_df.to_parquet(output_parquet_path, index=False)
    print(f"Successfully merged GeoParquets into {output_parquet_path}")

    return


def combine_final_outputs(primary_dir, secondary_dir, label):

    print('Combining CatFIM outputs from primary and secondary directories...')

    # Confirm that the primary_dir and secondary_dir exist
    if not os.path.exists(primary_dir):
        raise FileNotFoundError(f"Primary directory does not exist: {primary_dir}")

    if not os.path.exists(secondary_dir):
        raise FileNotFoundError(f"Secondary directory does not exist: {secondary_dir}")

    # Confirm that primary and secondary dir both have the same values for CATIM_TYPE FIM_RUN_DIR, PAST_MAJOR_INTERVAL_CAP, and SEARCH
    # (all found in the runtime_args.env file of the directories)

    # Get catfim_type_name from the runtime_args.env file in the primary_dir
    csf.load_runtime_args(primary_dir)
    catfim_type_primary = os.getenv('CATFIM_TYPE')
    fim_run_dir_primary = os.getenv('FIM_RUN_DIR')
    past_major_interval_cap_primary = os.getenv('PAST_MAJOR_INTERVAL_CAP')
    search_primary = os.getenv('SEARCH')

    # Get catfim_type_name from the runtime_args.env file in the secondary_dir
    csf.load_runtime_args(secondary_dir)
    catfim_type_secondary = os.getenv('CATFIM_TYPE')
    fim_run_dir_secondary = os.getenv('FIM_RUN_DIR')
    past_major_interval_cap_secondary = os.getenv('PAST_MAJOR_INTERVAL_CAP')
    search_secondary = os.getenv('SEARCH')

    # Confirm that the values are the same
    if catfim_type_primary != catfim_type_secondary:
        raise ValueError(
            f"CATFIM_TYPE values differ between directories: {catfim_type_primary} vs {catfim_type_secondary}"
        )
    if fim_run_dir_primary != fim_run_dir_secondary:
        raise ValueError(
            f"FIM_RUN_DIR values differ between directories: {fim_run_dir_primary} vs {fim_run_dir_secondary}"
        )
    if past_major_interval_cap_primary != past_major_interval_cap_secondary:
        raise ValueError(
            f"PAST_MAJOR_INTERVAL_CAP values differ between directories: {past_major_interval_cap_primary} vs {past_major_interval_cap_secondary}"
        )
    if search_primary != search_secondary:
        raise ValueError(f"SEARCH values differ between directories: {search_primary} vs {search_secondary}")

    if catfim_type_primary == 'sb':
        catfim_type_name = "stage_based"
    else:
        catfim_type_name = "flow_based"

    # Get output filepaths for the directories
    (
        sites_gpkg_path_primary,
        sites_csv_path_primary,
        sites_parquet_path_primary,
        library_gpkg_path_primary,
        library_csv_path_primary,
        library_parquet_path_primary,
    ) = cpp.get_output_filepaths(primary_dir, catfim_type_name)
    (
        sites_gpkg_path_secondary,
        sites_csv_path_secondary,
        sites_parquet_path_secondary,
        library_gpkg_path_secondary,
        library_csv_path_secondary,
        library_parquet_path_secondary,
    ) = cpp.get_output_filepaths(secondary_dir, catfim_type_name)

    # Merge GPKGs
    merge_gpkgs(
        sites_gpkg_path_primary,
        sites_gpkg_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_sites_{label}.gpkg'),
    )
    merge_gpkgs(
        library_gpkg_path_primary,
        library_gpkg_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_library_{label}.gpkg'),
    )

    # Merge CSVs
    merge_csvs(
        sites_csv_path_primary,
        sites_csv_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_sites_{label}.csv'),
    )
    merge_csvs(
        library_csv_path_primary,
        library_csv_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_library_{label}.csv'),
    )

    # Merge GeoParquets
    merge_geoparquets(
        sites_parquet_path_primary,
        sites_parquet_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_sites_{label}.parquet'),
    )
    merge_geoparquets(
        library_parquet_path_primary,
        library_parquet_path_secondary,
        os.path.join(primary_dir, f'{catfim_type_name}_catfim_library_{label}.parquet'),
    )

    print(
        'Successfully combined CatFIM outputs from primary and secondary directories into new files in the primary directory.'
    )

    return


if __name__ == '__main__':
    '''
    Joins the CatFIM outputs from a secondary folder to the outputs in a primary folder. The outputs are merged into new files in the primary folder with a label added to the filename.

    Arguments
    ----------
    primary-dir (-p) - str
        REQUIRED: Path to directory containing primary CatFIM outputs
    secondary-dir (-s) - str
        REQUIRED: Path to directory containing secondary CatFIM outputs (to be joined to the primary)
    label (-l) - str
        REQUIRED: Label for the output files (to differentiate them from the original primary outputs)

    Example
    -------

    python /foss_fim/tools/catfim/catfim_combine_final_outputs.py -p /data/catfim/emily_test/4_9_20_1_stage_based -s /data/catfim/emily_test/guam_4_9_20_1_stage_based/ -l 'w_Guam'


    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Join CatFIM outputs from primary and secondary directories')

    parser.add_argument(
        '-p',
        '--primary-dir',
        help='REQUIRED: Path to directory containing primary CatFIM outputs',
        required=True,
    )

    parser.add_argument(
        '-s',
        '--secondary-dir',
        help='REQUIRED: Path to directory containing secondary CatFIM outputs (to be joined to the primary)',
        required=True,
    )

    parser.add_argument(
        '-l',
        '--label',
        help='REQUIRED: Label for the output files (to differentiate them from the original primary outputs)',
        required=True,
    )

    args = vars(parser.parse_args())

    # Call main program
    combine_final_outputs(**args)
