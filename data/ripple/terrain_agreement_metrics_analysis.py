#!/usr/bin/env python3

import datetime as dt
import os
import re
import sqlite3
import traceback
from argparse import ArgumentParser
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv


metrics_dir = '/outputs/test_blacklist_metrics/collections/'
out_dir = '/outputs/test_blacklist_metrics/outputs_metrics/'
ripple_collection_name = 'mip_07140102'


# -----------------------------------------------------------------------------
def merge_nwm_streams_with_ripples(metrics_dir, out_dir, ripple_collection_name):

    # Load nwm_streams gpkg
    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    pre_clip_huc_dir = os.getenv("pre_clip_huc_dir")

    huc = str(re.search(r'\d+', ripple_collection_name).group(0))

    print(f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n')
    log_text = f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n'

    nwm_stream_gpkg = os.path.join(pre_clip_huc_dir, huc, 'nwm_subset_streams.gpkg')
    ripple_gpkg = os.path.join(metrics_dir, ripple_collection_name, 'ripple.gpkg')

    # Load ripple geopackage
    if os.path.exists(ripple_gpkg):
        ## List all layers in the GeoPackage
        # layers = fiona.listlayers(ripple_gpkg)

        # Read just "reaches" layer
        layer_name1 = "reaches"
        rip_reaches_gdf = gpd.read_file(ripple_gpkg, layer=layer_name1)
        rip_reaches_gdf = rip_reaches_gdf.rename(columns={'reach_id': 'feature_id'})

        # Read just "processing" layer
        con = sqlite3.connect(ripple_gpkg)
        rip_process_gdf = pd.read_sql_query("SELECT * FROM processing", con)
        con.close()
        rip_process_gdf = rip_process_gdf.rename(columns={'reach_id': 'feature_id'})
        rip_process_gdf = rip_process_gdf[['feature_id', 'collection_id', 'model_id']]

        # Merge ripple layers
        ripple_gdf = rip_reaches_gdf.merge(rip_process_gdf, on='feature_id', how='left')
        ripple_gdf = ripple_gdf.replace('', np.nan)
        ripple_gdf = ripple_gdf.dropna(subset=['model_id'])

        if os.path.exists(nwm_stream_gpkg):
            # Read just "reaches" layer
            nwms_gdf = gpd.read_file(nwm_stream_gpkg)
            nwms_gdf = nwms_gdf.rename(columns={'ID': 'feature_id'})

            columns_to_keep = ['feature_id', 'order_']  # , 'geometry'
            nwms_gdf = nwms_gdf[columns_to_keep]

            # Merge nwm_streams with ripple streams
            ripple_reaches_gdf = ripple_gdf.merge(nwms_gdf, on='feature_id', how='left')

            # Add a column to tag feature_id
            ripple_reaches_gdf['is_blacklisted'] = False
            ripple_reaches_gdf['is_valid'] = False

            geom_col1 = (
                ripple_reaches_gdf.geometry.name
            )  # gets the name of the current active geometry column
            cols32 = [col32 for col32 in ripple_reaches_gdf.columns if col32 != geom_col1] + [geom_col1]
            ripple_reaches_gdf = ripple_reaches_gdf[cols32]

            # Assign collection_id
            ripple_reaches_gdf['collection_id'] = np.where(
                ripple_reaches_gdf['model_id'].notna(), ripple_collection_name, None
            )

            # Create the HUC folder
            huc_out_folder = os.path.join(out_dir, ripple_collection_name)
            os.makedirs(huc_out_folder, exist_ok=True)

            # Save as a new GeoPackage
            path_ripple_reaches = os.path.join(
                huc_out_folder, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
            )
            if not os.path.exists(path_ripple_reaches):
                ripple_reaches_gdf.to_file(path_ripple_reaches, driver="GPKG")

    return log_text


# -----------------------------------------------------------------------------
def merge_ripple_reaches_sourcemodels_with_metrics_db(metrics_dir, out_dir, ripple_collection_name):

    huc = re.search(r'\d+', ripple_collection_name).group(0)

    # Read metrics geopackage
    path_ripple_collection_out = os.path.join(out_dir, ripple_collection_name)
    path_ripple_reaches = os.path.join(
        path_ripple_collection_out, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
    )

    log_text = ''
    if not os.path.exists(path_ripple_reaches):
        log_text += merge_nwm_streams_with_ripples(metrics_dir, out_dir, ripple_collection_name)

    print(f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n')
    log_text += f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n'

    # Read metrics database
    dataset_dir = os.path.join(metrics_dir, ripple_collection_name)  # , 'submodels')
    db_paths = list(Path(dataset_dir).rglob("*.db"))
    # corr_matrix = None
    if len(db_paths) == 0:
        print(f'{ripple_collection_name} does not have any metrics database\n')
        log_text += f'{ripple_collection_name} does not have any metrics database\n'

    else:
        model_metrics_ls = []
        for dbpi in db_paths:

            feature_id = Path(dbpi).stem.split('.')[0]
            parts = dbpi.parts
            idx = parts.index('collections')
            relative_db_path = Path(*parts[idx + 1 : -1])
            relative_db_path = str(relative_db_path)

            # Connect to your SQLite .db file
            conn2 = sqlite3.connect(dbpi)
            cursor2 = conn2.cursor()
            cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")

            # Read a table or SQL query into a DataFrame
            mm_df = pd.read_sql_query("SELECT * FROM model_metrics", conn2)
            # xsm_df = pd.read_sql_query("SELECT * FROM model_metrics", conn)

            # Add a column to indicate source database
            mm_df['feature_id'] = int(feature_id)
            mm_df['db_path'] = relative_db_path
            model_metrics_ls.append(mm_df)
            conn2.close()

        # Concatenate all DataFrames
        model_metrics_df = pd.concat(model_metrics_ls, ignore_index=True)

        path_metrics_table_huc = os.path.join(
            path_ripple_collection_out, f'ripple_reaches_sourcemodels_metrics_{huc}.csv'
        )
        if not os.path.exists(path_metrics_table_huc):
            model_metrics_df.to_csv(path_metrics_table_huc, index=False)

        if os.path.exists(path_ripple_reaches) and os.path.exists(path_metrics_table_huc):

            ripple_reaches_submod_gdf_d = gpd.read_file(path_ripple_reaches)
            ripple_reaches_submod_gdf = ripple_reaches_submod_gdf_d.drop_duplicates()

            # Merge ripple_reaches_gdf with model_metrics_df
            ripple_reaches_metrics_gdf = ripple_reaches_submod_gdf.merge(
                model_metrics_df, on='feature_id', how='left'
            )
            geom_col = (
                ripple_reaches_metrics_gdf.geometry.name
            )  # gets the name of the current active geometry column
            cols = [col for col in ripple_reaches_metrics_gdf.columns if col != geom_col] + [geom_col]
            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf[cols]

            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.replace('', np.nan)
            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.dropna(subset=['avg_inundation_overlap'])

            path_ripple_reaches_metrics = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_order_source_models_metrics_{huc}.gpkg'
            )
            if not os.path.exists(path_ripple_reaches_metrics):
                ripple_reaches_metrics_gdf.to_file(path_ripple_reaches_metrics)

    return log_text


# -----------------------------------------------------------------------------
def create_ripple_STREAMS_gdf_csv(metrics_dir, out_dir):

    ripple_collections = [d for d in os.listdir(metrics_dir) if os.path.isdir(os.path.join(metrics_dir, d))]
    ripple_collections.sort()

    print(f'{len(ripple_collections)} ripple collections have been found to analyze.\n')
    log_text = f'{len(ripple_collections)} ripple collections have been found to analyze.\n'

    # model_metrics_corr_ls = []
    metrics_streams_conus_ls = []
    metrics_streams_conus_gpkg_ls = []
    metrics_reaches_conus_ls = []
    all_sourcemodels_reaches_ls = []
    for rmi in range(len(ripple_collections)):
        try:
            # ripple_collections[rmi] = 'mip_05130202'
            huc = re.search(r'\d+', ripple_collections[rmi]).group(0)
            log_text += f'Start analyzing ripple collections for HUC {huc}\n'

            # Read metrics geopackage
            path_ripple_collection_out = os.path.join(out_dir, ripple_collections[rmi])
            path_ripple_reaches = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_order_source_models_metrics_{huc}.gpkg'
            )

            if not os.path.exists(path_ripple_reaches):
                log_text_m = merge_ripple_reaches_sourcemodels_with_metrics_db(
                    metrics_dir, out_dir, ripple_collections[rmi]
                )
                log_text += log_text_m

            path_sourcemodels_gpkg = os.path.join(
                path_ripple_collection_out, f"ripple_reaches_order_sourcemodels_{huc}.gpkg"
            )
            path_metrics_csv = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_sourcemodels_metrics_{huc}.csv'
            )

            if os.path.exists(path_sourcemodels_gpkg):
                sourcemodels_gdf = gpd.read_file(path_sourcemodels_gpkg)

                if 'is_blacklisted' not in sourcemodels_gdf.columns:
                    sourcemodels_gdf['is_blacklisted'] = False

                if 'is_valid' not in sourcemodels_gdf.columns:
                    sourcemodels_gdf['is_valid'] = False

                sourcemodels_df = sourcemodels_gdf.drop(columns=['geometry'])
                sourcemodels_df['huc'] = huc

                if os.path.exists(path_metrics_csv):
                    metric_df = pd.read_csv(path_metrics_csv)

                    if 'db_path' in metric_df.columns:
                        db_path_df = metric_df[['feature_id', 'db_path']].drop_duplicates()

                        sourcemodels_df['feature_id'] = pd.to_numeric(
                            sourcemodels_df['feature_id'], errors='coerce'
                        ).astype('Int64')

                        db_path_df['feature_id'] = pd.to_numeric(
                            db_path_df['feature_id'], errors='coerce'
                        ).astype('Int64')

                        sourcemodels_df = sourcemodels_df.merge(db_path_df, on='feature_id', how='left')
                else:
                    sourcemodels_df['db_path'] = None

                all_sourcemodels_reaches_ls.append(sourcemodels_df)

            ripple_reaches_metrics_gdf_d = gpd.read_file(path_ripple_reaches)
            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf_d.drop_duplicates()

            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.replace('', np.nan)
            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.dropna(subset=['avg_inundation_overlap'])

            ripple_reaches_metrics_df = ripple_reaches_metrics_gdf.drop(columns=['geometry'])
            metrics_reaches_conus_ls.append(ripple_reaches_metrics_df)

            # Averaging by source models/streams
            # grouped_avg = gdf.groupby('col10')[['col1', 'col2', 'col3']].mean().reset_index()

            # Identify numeric columns
            numeric_cols = ripple_reaches_metrics_gdf.select_dtypes(include='number').columns.tolist()

            # Specify which numeric columns get 'max'
            max_cols = ['feature_id', 'nwm_to_id', 'order_']

            # Columns to average = numeric columns excluding col1 and col2
            mean_cols = [col for col in numeric_cols if col not in max_cols]

            # Identify non-numeric columns (excluding geometry)
            # non_numeric_cols = ripple_reaches_metrics_gdf.select_dtypes(exclude='number').columns.tolist()
            non_numeric_cols = ['collection_id']
            # Remove geometry column if present (geometry handled automatically)
            non_numeric_cols = [
                col for col in non_numeric_cols if col != ripple_reaches_metrics_gdf.geometry.name
            ]

            # Build the aggfunc dictionary
            aggfunc = {col: 'max' for col in max_cols}
            aggfunc.update({col: 'mean' for col in mean_cols})
            aggfunc.update({col: 'first' for col in non_numeric_cols})

            # Now dissolve with this aggregation
            metrics_streams_gdf = ripple_reaches_metrics_gdf.dissolve(
                by='model_id', aggfunc=aggfunc
            ).reset_index()
            metrics_streams_gdf['huc'] = [huc] * len(metrics_streams_gdf)

            # Move the geometry to the end
            # List all columns except geometry
            first_cols = ['huc', 'collection_id', 'model_id']

            # Make a list of remaining columns, excluding these and geometry
            remaining_cols = [
                col
                for col in metrics_streams_gdf.columns
                if col not in first_cols and col != metrics_streams_gdf.geometry.name
            ]
            # Append geometry column at the end
            new_order = first_cols + remaining_cols + [metrics_streams_gdf.geometry.name]

            # Reorder the GeoDataFrame
            metrics_streams_gdf = metrics_streams_gdf[new_order]
            metrics_streams_gdf = metrics_streams_gdf.replace('', np.nan)
            metrics_streams_gdf = metrics_streams_gdf.dropna(subset=['avg_inundation_overlap'])

            # Save the gdf
            path_streams_metrics = os.path.join(path_ripple_collection_out, f'streams_metrics_{huc}.gpkg')
            if not os.path.exists(path_streams_metrics):
                metrics_streams_gdf.to_file(path_streams_metrics)

            metrics_streams_conus_gpkg_ls.append(metrics_streams_gdf)

            metrics_streams_df = metrics_streams_gdf.drop(columns=['geometry'])
            metrics_streams_conus_ls.append(metrics_streams_df)

        except Exception as e:

            error_msg = f"Error processing folder {ripple_collections[rmi]}: {str(e)}\n"
            print(error_msg)
            log_text += error_msg
            continue

    # Save reaches matrix conus wise in csv format
    metrics_reaches_conus_df = pd.concat(metrics_reaches_conus_ls, axis=0, ignore_index=True)
    metrics_reaches_conus_df = metrics_reaches_conus_df.replace('', np.nan)
    metrics_reaches_conus_df = metrics_reaches_conus_df.dropna(subset=['avg_inundation_overlap'])

    path_metrics_reaches_conus = os.path.join(out_dir, 'metrics_reaches_ripple_submodels_conus.csv')
    if not os.path.exists(path_metrics_reaches_conus):
        metrics_reaches_conus_df.to_csv(path_metrics_reaches_conus, index=False)

    # Save stream matrics conus wise in gpkg format
    metrics_streams_conus_gpkg = pd.concat(metrics_streams_conus_gpkg_ls, axis=0, ignore_index=True)
    metrics_streams_conus_gpkg = metrics_streams_conus_gpkg.replace('', np.nan)
    metrics_streams_conus_gpkg = metrics_streams_conus_gpkg.dropna(subset=['avg_inundation_overlap'])

    path_metrics_streams_conus_gpkg = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.gpkg')
    if not os.path.exists(path_metrics_streams_conus_gpkg):
        metrics_streams_conus_gpkg.to_file(path_metrics_streams_conus_gpkg, index=False)

    # Save stream matrix conus wise in csv formats
    metrics_streams_conus = pd.concat(metrics_streams_conus_ls, axis=0, ignore_index=True)
    metrics_streams_conus = metrics_streams_conus.replace('', np.nan)
    metrics_streams_conus = metrics_streams_conus.dropna(subset=['avg_inundation_overlap'])

    path_metrics_streams_conus = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.csv')
    if not os.path.exists(path_metrics_streams_conus):
        metrics_streams_conus.to_csv(path_metrics_streams_conus, index=False)

    if len(all_sourcemodels_reaches_ls) > 0:
        all_sourcemodeles_conus_df = pd.concat(all_sourcemodels_reaches_ls, axis=0, ignore_index=True)
        path_all_sourcemodels_conus = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.csv')
        all_sourcemodeles_conus_df.to_csv(path_all_sourcemodels_conus, index=False)

    return log_text


# -----------------------------------------------------------------------------
def process_ripple_STREAMS_create_blackList(out_dir):

    path_ripple_streams = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.csv')
    path_ripple_reaches = os.path.join(out_dir, 'metrics_reaches_ripple_submodels_conus.csv')

    log_text = ''
    if not os.path.exists(path_ripple_streams):
        log_text += create_ripple_STREAMS_gdf_csv(out_dir)

    else:
        log_text += 'Ripple streams matrics csv file already exists ...\n'
        print('Ripple streams matrics csv file already exists ...\n')

    print('Start creating the black list ...\n')
    log_text += 'Start creating the black list ...\n'

    ripple_streams_metrics_df = gpd.read_csv(path_ripple_streams)
    ripple_streams_metrics_df = ripple_streams_metrics_df.replace('', np.nan)
    ripple_streams_metrics_df = ripple_streams_metrics_df.dropna(subset=['avg_inundation_overlap'])

    numeric_columns = [
        'order_',
        'avg_inundation_overlap',
        'avg_flow_area_overlap',
        'avg_top_width_agreement',
        'avg_flow_area_agreement',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        'avg_spectral_angle',
        'avg_spectral_correlation',
        'avg_correlation',
        'avg_max_cross_correlation',
        'avg_thalweg_elevation_difference',
    ]
    ripple_streams_metrics_df[numeric_columns] = ripple_streams_metrics_df[numeric_columns].apply(
        pd.to_numeric, errors='coerce'
    )
    # feature_ids with low inundation_overlap metric
    mask1 = (ripple_streams_metrics_df['avg_thalweg_elevation_difference'] >= 100) | (
        ripple_streams_metrics_df['avg_thalweg_elevation_difference'] <= -50
    )
    outlier_fid_thalweg_elev_diff = ripple_streams_metrics_df[mask1]

    mask2 = ripple_streams_metrics_df['avg_inundation_overlap'] <= 0.3
    low_inundation_overlap = ripple_streams_metrics_df[mask2]

    mask3 = (
        (ripple_streams_metrics_df['order_'] <= 3)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] < 0.55)
        & (ripple_streams_metrics_df['avg_r_squared'] < 0.6)
    )
    low_r2_1 = ripple_streams_metrics_df[mask3]

    mask4 = (
        (ripple_streams_metrics_df['order_'] >= 4)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] < 0.5)
        & (ripple_streams_metrics_df['avg_r_squared'] < 0.52)
    )
    low_r2_2 = ripple_streams_metrics_df[mask4]

    # avg_inundation_overlap < = 0.4 and
    # avg_hydraulic_radius_agreement < 0.52
    mask5 = (
        (ripple_streams_metrics_df['order_'] <= 3)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] <= 0.5)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] <= 0.52)
    )
    outlier_fid_hr_fim_1 = ripple_streams_metrics_df[mask5]
    mask6 = (
        (ripple_streams_metrics_df['order_'] >= 4)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] < 0.4)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] < 0.45)
    )
    outlier_fid_hr_fim_2 = ripple_streams_metrics_df[mask6]

    # avg_inundation_overlap < = 0.45 and
    # avg_hydraulic_radius_agreement < 0.5 and
    # avg_thalweg_elevation_difference < -4.2
    mask7 = (
        (ripple_streams_metrics_df['order_'] <= 3)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] <= 0.45)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] <= 0.52)
        & (ripple_streams_metrics_df['avg_thalweg_elevation_difference'] <= -4.2)
    )
    outlier_fid_thalweg_hr_fim1 = ripple_streams_metrics_df[mask7]
    mask8 = (
        (ripple_streams_metrics_df['order_'] >= 4)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] <= 0.40)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] <= 0.5)
        & (ripple_streams_metrics_df['avg_thalweg_elevation_difference'] <= -10)
    )
    outlier_fid_thalweg_hr_fim2 = ripple_streams_metrics_df[mask8]

    # avg_inundation_overlap < = 0.6 and
    # avg_hydraulic_radius_agreement < 0.6 and
    # avg_thalweg_elevation_difference < -10
    mask9 = (
        (ripple_streams_metrics_df['order_'] < 3)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] < 0.55)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] < 0.6)
        & (ripple_streams_metrics_df['avg_thalweg_elevation_difference'] <= -10)
    )
    outlier_fid_thalweg_hr_fim3 = ripple_streams_metrics_df[mask9]

    mask10 = (
        (ripple_streams_metrics_df['order_'] >= 3)
        & (ripple_streams_metrics_df['avg_inundation_overlap'] < 0.51)
        & (ripple_streams_metrics_df['avg_hydraulic_radius_agreement'] < 0.55)
        & (ripple_streams_metrics_df['avg_thalweg_elevation_difference'] <= -45)
    )
    outlier_fid_thalweg_hr_fim4 = ripple_streams_metrics_df[mask10]

    outlier_streams_conus_df = pd.concat(
        [
            outlier_fid_thalweg_elev_diff,
            low_inundation_overlap,
            low_r2_1,
            low_r2_2,
            outlier_fid_hr_fim_1,
            outlier_fid_hr_fim_2,
            outlier_fid_thalweg_hr_fim1,
            outlier_fid_thalweg_hr_fim2,
            outlier_fid_thalweg_hr_fim3,
            outlier_fid_thalweg_hr_fim4,
        ],
        axis=0,
        ignore_index=True,
    )

    col_to_front = [
        'huc',
        'collection_id',
        'model_id',
        'feature_id',
        'order_',
        'avg_inundation_overlap',
        'avg_thalweg_elevation_difference',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        'avg_spectral_angle',
    ]
    cols_rearranged = col_to_front + [c for c in outlier_streams_conus_df.columns if c not in col_to_front]
    outlier_streams_conus_df_d = outlier_streams_conus_df[cols_rearranged]
    outlier_streams_conus_df = outlier_streams_conus_df_d.drop_duplicates()

    num_outlier_streams_conus = len(outlier_streams_conus_df)
    print(f'Number of the outlier Ripple models is {num_outlier_streams_conus}\n')
    log_text += f'Number of the outlier Ripple models is {num_outlier_streams_conus}\n'

    path_outlier_streams_conus = os.path.join(out_dir, 'outlier_streams_conus.csv')
    outlier_streams_conus_df.to_csv(path_outlier_streams_conus, index=False)

    if os.path.exists(path_ripple_reaches):
        ripple_reaches_metrics_df = gpd.read_file(path_ripple_reaches)
        ripple_streams_metrics_df = ripple_streams_metrics_df.replace('', np.nan)
        ripple_reaches_metrics_df = ripple_reaches_metrics_df.dropna(subset=['avg_inundation_overlap'])

    outlier_cols = [
        'collection_id',
        'model_id',
        'feature_id',
        'avg_inundation_overlap',
        'avg_thalweg_elevation_difference',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        'huc',
    ]
    outlier_reaches_conus_df = ripple_reaches_metrics_df.merge(
        outlier_streams_conus_df[outlier_cols], on=['collection_id', 'model_id'], how='inner'
    )

    outlier_reaches_conus_df = outlier_reaches_conus_df.drop_duplicates()
    outlier_reaches_conus_df = outlier_reaches_conus_df.rename(
        columns={
            'feature_id_x': 'feature_id',
            'avg_inundation_overlap_y': 'avg_inundation_overlap',
            'avg_thalweg_elevation_difference_y': 'avg_thalweg_elevation_difference',
            'avg_hydraulic_radius_agreement_y': 'avg_hydraulic_radius_agreement',
            'avg_r_squared_y': 'avg_r_squared',
            'avg_inundation_overlap_x': 'inundation_overlap',
            'avg_thalweg_elevation_difference_x': 'thalweg_elevation_difference',
            'avg_hydraulic_radius_agreement_x': 'hydraulic_radius_agreement',
            'avg_r_squared_x': 'r_squared',
        }
    )

    outlier_reaches_conus_df['inundation_overlap'] = outlier_reaches_conus_df['inundation_overlap'].replace(
        '', np.nan
    )
    outlier_reaches_conus_df = outlier_reaches_conus_df.dropna(subset=['inundation_overlap'])
    path_outlier_reaches_conus = os.path.join(out_dir, 'outlier_reaches_conus.csv')
    outlier_reaches_conus_df.to_csv(path_outlier_reaches_conus, index=False)

    path_all_sourcemodels_conus = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.csv')
    if os.path.exists(path_all_sourcemodels_conus):
        print('Creating master CONUS whitelist csv...\n')
        log_text += 'Creating master CONUS whitelist csv...\n'
        all_sourcemodels_conus_df = pd.read_csv(path_all_sourcemodels_conus)

        bad_features = outlier_reaches_conus_df[['collection_id', 'feature_id']].drop_duplicates()
        bad_features['is_bad'] = True
        bad_features['feature_id'] = pd.to_numeric(bad_features['feature_id'], errors='coerce').astype(
            'Int64'
        )
        all_sourcemodels_conus_df['feature_id'] = pd.to_numeric(
            all_sourcemodels_conus_df['feature_id'], errors='coerce'
        ).astype('Int64')

        # Merge
        merged_all = all_sourcemodels_conus_df.merge(
            bad_features, on=['collection_id', 'feature_id'], how='left'
        )

        merged_all['is_blacklisted'] = merged_all['is_bad'].fillna(False)
        merged_all = merged_all.drop(columns=['is_bad'])
        path_master_whitelist_conus = os.path.join(out_dir, 'ripple_feature_id_whitelist_conus.csv')
        merged_all.to_csv(path_master_whitelist_conus, index=False)

    log_text += 'Successfully created a blacklist of the Ripple models from the provided collections. \n'
    print('Successfully created a blacklist of the Ripple models from the provided collections. \n')

    return log_text


# -----------------------------------------------------------------------------
# Apply ripple_streams_blacklist function on metrics_dir
def apply_ripple_streams_blacklist(metrics_dir, out_dir, log_file_path):
    """
    Function for processing ripple STREAMS and create a black list of bad models.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        metrics_dir: str

        Returns
        ----------
        log_text : str
    """
    log_text = ""
    try:
        msg = "Processing ripple STREAMS and create a black list\n"
        log_text += msg
        print(msg)
        log_text += process_ripple_STREAMS_create_blackList(metrics_dir, out_dir)

    except Exception:
        log_text += "An error has occurred while processing ripple STREAMS"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -----------------------------------------------------------------------------
def log_create_blacklist(metrics_dir, out_dir):
    """
    Function for correcting synthetic rating curves using Multi-Proc approach.
    It will correct each branch's SRCs in serial based on the HydroIDs.

        Parameters
        ----------
        metrics-dir : str
            Directory path for saved ripple matrics.

    """
    # Set up log file
    # log_file_path = os.path.join(metrics_dir, 'process_ripple_STREAMS' + '.log')
    # print(f'Writing progress to log file here: {log_file_path}')
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)
    timestamp = begin_time.strftime("%Y%m%d_%H%M%S")
    log_file_name = f"process_ripple_STREAMS_{timestamp}.log"
    log_file_path = os.path.join(out_dir, log_file_name)
    print(f'Writing progress to log file here: {log_file_path}')

    ## Initiate log file
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    msg = "Creating a black list of ripple streams\n"
    log_text += msg

    apply_ripple_streams_blacklist(metrics_dir, out_dir, log_file_path)

    ## Record run time and close log file
    end_time = dt.datetime.now(dt.timezone.utc)
    log_text += 'END TIME: ' + str(end_time) + '\n'
    tot_run_time = end_time - begin_time
    log_text += 'TOTAL RUN TIME: ' + str(tot_run_time).split('.')[0]
    log_file.close()


if __name__ == '__main__':

    """
    Parameters
    ----------
    metrics-dir : str
        Directory path for saved ripple matrics.

    Sample usage:
    python3 /data/ripple/terrain_agreement_metrics_analysis.py
    -md /outputs/NGWPC-tasks/terrain-agreements/test_pr/metrics_dir1/

    Note: You need to connect to the FIM docker to run this code.

    """
    parser = ArgumentParser(description="Process Ripple Streams and Create a Black List")
    parser.add_argument('-md', '--metrics-dir', help='saved ripple matrics dir', required=True, type=str)
    parser.add_argument('-od', '--out-dir', help='saved output dir', required=True, type=str)

    args = vars(parser.parse_args())

    metrics_dir = args['metrics_dir']
    out_dir = args['out_dir']

    # parent_dir = os.path.dirname(os.path.normpath(metrics_dir))
    # out_dir = os.path.join(parent_dir, 'ripple_metrics')
    # os.makedirs(out_dir, exist_ok=True)

    log_create_blacklist(metrics_dir, out_dir)
