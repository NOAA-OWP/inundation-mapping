#!/usr/bin/env python3

import datetime as dt
import os
import re
import sqlite3
import traceback
from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv


# import matplotlib.pyplot as plt


# ***************************************************************
def count_db_files(directory):
    count = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.db'):
                count += 1
    return count


# ***************************************************************
def count_num_models_reaches_metrix(metrix_dir):

    num_db_files = count_db_files(metrix_dir)
    print(f"Number of .db files: {num_db_files}")

    ripple_models = [d for d in os.listdir(metrix_dir) if os.path.isdir(os.path.join(metrix_dir, d))]
    ripple_models.sort()

    num_models_metrix_df = pd.DataFrame()
    indexc = 0
    num_models_metrix = 0
    for rmi in range(len(ripple_models)):  # [100:133]
        huc = re.search(r'\d+', ripple_models[rmi]).group(0)

        path_ripple_collection = os.path.join(metrix_dir, ripple_models[rmi])
        path_ripple_reaches_metrix = os.path.join(
            path_ripple_collection, f'ripple_reaches_order_source_models_metrix_{huc}.gpkg'
        )

        if os.path.exists(path_ripple_reaches_metrix):

            ripple_reaches_metrix_gdf_d = gpd.read_file(path_ripple_reaches_metrix)
            ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf_d.drop_duplicates()
            # print(ripple_reaches_metrix_gdf[~ripple_reaches_metrix_gdf['model_id'].isna()])

            # Counting number of models/feature-ids that have metrics
            num_rows_not_nan = ripple_reaches_metrix_gdf['avg_inundation_overlap'].notna().sum()
            num_models_metrix_df.loc[indexc, '#notnan_metrics'] = int(num_rows_not_nan)
            indexc += 1
            num_models_metrix += num_rows_not_nan

        else:
            num_rows_not_nan = 0
            num_models_metrix_df.loc[indexc, '#notnan_metrics'] = int(num_rows_not_nan)
            indexc += 1
            num_models_metrix += num_rows_not_nan

    num_models_metrix_df = num_models_metrix_df.fillna(0)
    path_num_models_metrix = os.path.join(metrix_dir, 'num_models_metrix.csv')
    num_models_metrix_df.to_csv(path_num_models_metrix)


# ***************************************************************
def merge_nwm_streams_with_ripples(metrix_dir, ripple_model_name):

    srcDir = os.getenv('srcDir')
    load_dotenv(f'{srcDir}/bash_variables.env')
    pre_clip_huc_dir = os.getenv("pre_clip_huc_dir")

    huc = str(re.search(r'\d+', ripple_model_name).group(0))
    print(pre_clip_huc_dir)

    print(f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n')
    log_text = f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n'

    nwm_stream_gpkg = os.path.join(pre_clip_huc_dir, huc, 'nwm_subset_streams.gpkg')
    ripple_gpkg = os.path.join(metrix_dir, ripple_model_name, 'ripple.gpkg')

    # Read ripple geopackage
    if os.path.exists(ripple_gpkg):
        ## List all layers in the GeoPackage
        # layers = fiona.listlayers(ripple_gpkg)
        # print(layers)  # Find the correct layer name for your shapefile

        # Read just "reaches" layer
        layer_name1 = "reaches"
        gdf_rip_reaches = gpd.read_file(ripple_gpkg, layer=layer_name1)
        gdf_rip_reaches = gdf_rip_reaches.rename(columns={'reach_id': 'feature_id'})

        # Read just "processing" layer
        con = sqlite3.connect(ripple_gpkg)
        gdf_rip_process0 = pd.read_sql_query("SELECT * FROM processing", con)
        con.close()
        gdf_rip_process0 = gdf_rip_process0.rename(columns={'reach_id': 'feature_id'})
        gdf_rip_process = gdf_rip_process0[['feature_id', 'collection_id', 'model_id']]

        # Merge ripple layers
        gdf_ripple = gdf_rip_reaches.merge(gdf_rip_process, on='feature_id', how='left')
        gdf_ripple = gdf_ripple.replace('', np.nan)
        gdf_ripple = gdf_ripple.dropna(subset=['model_id'])

        if os.path.exists(nwm_stream_gpkg):
            # Read just "reaches" layer
            gdf_nwms = gpd.read_file(nwm_stream_gpkg)
            gdf_nwms = gdf_nwms.rename(columns={'ID': 'feature_id'})

            columns_to_keep = ['feature_id', 'order_']  # , 'geometry'
            gdf_nwms = gdf_nwms[columns_to_keep]

            # Merge nwm_streams with ripple streams
            ripple_reaches_gdf = gdf_ripple.merge(gdf_nwms, on='feature_id', how='left')
            geom_col1 = (
                ripple_reaches_gdf.geometry.name
            )  # gets the name of the current active geometry column
            cols32 = [col32 for col32 in ripple_reaches_gdf.columns if col32 != geom_col1] + [geom_col1]
            ripple_reaches_gdf = ripple_reaches_gdf[cols32]

            # Assign collection_id
            ripple_reaches_gdf['collection_id'] = np.where(
                ripple_reaches_gdf['model_id'].notna(), ripple_model_name, None
            )

            # Save as a new GeoPackage
            path_ripple_reaches = os.path.join(
                metrix_dir, ripple_model_name, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
            )
            if not os.path.exists(path_ripple_reaches):
                ripple_reaches_gdf.to_file(path_ripple_reaches, driver="GPKG")

    return log_text


# ***************************************************************
def merge_ripple_reaches_sourcemodels_with_metrix_db(metrix_dir, ripple_model_name):

    # ripple_models[rmi] = 'mip_05130202'
    huc = re.search(r'\d+', ripple_model_name).group(0)

    # Read metrics geopackage
    path_ripple_collection = os.path.join(metrix_dir, ripple_model_name)
    path_ripple_reaches = os.path.join(
        path_ripple_collection, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
    )

    log_text = ''
    if not os.path.exists(path_ripple_reaches):
        log_text += merge_nwm_streams_with_ripples(metrix_dir, ripple_model_name)

    print(f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n')
    log_text += f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n'

    # Read metrics database
    dataset_dir = os.path.join(metrix_dir, ripple_model_name)  # , 'submodels')
    db_paths = list(Path(dataset_dir).rglob("*.db"))
    if len(db_paths) == 0:
        print(f'{ripple_model_name} does not have any metrics database\n')
        log_text += '{ripple_model_name} does not have any metrics database\n'

    else:
        model_metrix_ls = []
        for dbpi in db_paths:

            feature_id = Path(dbpi).stem.split('.')[0]

            # Connect to your SQLite .db file
            conn2 = sqlite3.connect(dbpi)
            cursor2 = conn2.cursor()
            cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")
            # print(cursor.fetchall())  # This prints a list of table names

            # Read a table or SQL query into a DataFrame
            mm_df = pd.read_sql_query("SELECT * FROM model_metrics", conn2)
            # xsm_df = pd.read_sql_query("SELECT * FROM model_metrics", conn)

            # Add a column to indicate source database
            mm_df['feature_id'] = int(feature_id)
            model_metrix_ls.append(mm_df)
            conn2.close()

        # Concatenate all DataFrames
        model_metrix_df = pd.concat(model_metrix_ls, ignore_index=True)

        path_metrix_table_huc = os.path.join(dataset_dir, f'ripple_reaches_sourcemodels_metrics_{huc}.csv')
        if not os.path.exists(path_metrix_table_huc):
            model_metrix_df.to_csv(path_metrix_table_huc)

        if os.path.exists(path_ripple_reaches) and os.path.exists(path_metrix_table_huc):

            ripple_reaches_submod_gdf_d = gpd.read_file(path_ripple_reaches)
            ripple_reaches_submod_gdf = ripple_reaches_submod_gdf_d.drop_duplicates()
            # Merge ripple_reaches_gdf with model_metrix_df

            ripple_reaches_metrix_gdf = ripple_reaches_submod_gdf.merge(
                model_metrix_df, on='feature_id', how='left'
            )
            geom_col = (
                ripple_reaches_metrix_gdf.geometry.name
            )  # gets the name of the current active geometry column
            cols = [col for col in ripple_reaches_metrix_gdf.columns if col != geom_col] + [geom_col]
            ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf[cols]

            ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf.replace('', np.nan)
            ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf.dropna(subset=['avg_inundation_overlap'])

            path_ripple_reaches_metrix = os.path.join(
                path_ripple_collection, f'ripple_reaches_order_source_models_metrix_{huc}.gpkg'
            )
            if not os.path.exists(path_ripple_reaches_metrix):
                ripple_reaches_metrix_gdf.to_file(path_ripple_reaches_metrix)
            # print(ripple_reaches_metrix_gdf[~ripple_reaches_metrix_gdf['model_id'].isna()])

            # Covariance of metrics
            corr_columns = [
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
            cov_matrix = ripple_reaches_metrix_gdf[corr_columns].cov().round(4)
            path_t0_save_cov = os.path.join(metrix_dir, ripple_model_name, f'ripple_reaches_cov_{huc}.csv')
            if not os.path.exists(path_t0_save_cov):
                cov_matrix.to_csv(path_t0_save_cov)

            # Correlation of metrics
            corr_matrix = ripple_reaches_metrix_gdf[corr_columns].corr().round(4)
            corr_matrix['ripple_model'] = ripple_model_name
            corr_matrix['HUC'] = huc

            cols_to_move = ['HUC', 'ripple_model']
            cols2 = corr_matrix.columns
            # Create the new order: the two columns first, then all others except these two
            new_order = cols_to_move + [col for col in cols2 if col not in cols_to_move]
            # Reorder the dataframe
            corr_matrix = corr_matrix[new_order]

            # model_metrix_corr_ls.append(corr_matrix)
            # print(corr_matrix)
            path_t0_save_corr = os.path.join(path_ripple_collection, f'ripple_reaches_order_corr_{huc}.csv')
            if not os.path.exists(path_t0_save_corr):
                corr_matrix.to_csv(path_t0_save_corr)

    return corr_matrix, log_text


# ***************************************************************
def create_ripple_STREAMS_gdf_csv(metrix_dir):

    ripple_models = [d for d in os.listdir(metrix_dir) if os.path.isdir(os.path.join(metrix_dir, d))]
    ripple_models.sort()

    print(f'{len(ripple_models)} ripple collections have been found to analyze.\n')
    log_text = f'{len(ripple_models)} ripple collections have been found to analyze.\n'

    model_metrix_corr_ls = []
    metrix_streams_conus_ls = []
    metrix_streams_conus_gpkg_ls = []
    metrix_reaches_conus_ls = []
    for rmi in range(len(ripple_models)):

        # ripple_models[rmi] = 'mip_05130202'
        huc = re.search(r'\d+', ripple_models[rmi]).group(0)
        log_text += f'Start analyzing ripple collections for HUC {huc}\n'

        # Read metrics geopackage
        path_ripple_collection = os.path.join(metrix_dir, ripple_models[rmi])
        path_ripple_reaches = os.path.join(
            path_ripple_collection, f'ripple_reaches_order_source_models_metrix_{huc}.gpkg'
        )

        if not os.path.exists(path_ripple_reaches):
            corr_matrix, log_text_m = merge_ripple_reaches_sourcemodels_with_metrix_db(
                metrix_dir, ripple_models[rmi]
            )
            log_text += log_text_m
            model_metrix_corr_ls.append(corr_matrix)

        else:
            corr_matrix_path = os.path.join(path_ripple_collection, f'ripple_reaches_order_corr_{huc}.csv')
            corr_matrix = pd.read_csv(corr_matrix_path)
            model_metrix_corr_ls.append(corr_matrix)

        ripple_reaches_metrix_gdf_d = gpd.read_file(path_ripple_reaches)
        ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf_d.drop_duplicates()

        ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf.replace('', np.nan)
        ripple_reaches_metrix_gdf = ripple_reaches_metrix_gdf.dropna(subset=['avg_inundation_overlap'])
        # print(ripple_reaches_metrix_gdf[~ripple_reaches_metrix_gdf['model_id'].isna()])

        ripple_reaches_metrix_df = ripple_reaches_metrix_gdf.drop(columns=['geometry'])
        metrix_reaches_conus_ls.append(ripple_reaches_metrix_df)

        # Averaging by source models/streams
        # grouped_avg = gdf.groupby('col10')[['col1', 'col2', 'col3']].mean().reset_index()
        # Identify numeric columns
        numeric_cols = ripple_reaches_metrix_gdf.select_dtypes(include='number').columns.tolist()

        # Specify which numeric columns get 'max'
        max_cols = ['feature_id', 'nwm_to_id', 'order_']

        # Columns to average = numeric columns excluding col1 and col2
        mean_cols = [col for col in numeric_cols if col not in max_cols]

        # Identify non-numeric columns (excluding geometry)
        # non_numeric_cols = ripple_reaches_metrix_gdf.select_dtypes(exclude='number').columns.tolist()
        non_numeric_cols = ['collection_id']
        # Remove geometry column if present (geometry handled automatically)
        non_numeric_cols = [col for col in non_numeric_cols if col != ripple_reaches_metrix_gdf.geometry.name]

        # Build the aggfunc dictionary
        aggfunc = {col: 'max' for col in max_cols}
        aggfunc.update({col: 'mean' for col in mean_cols})
        aggfunc.update({col: 'first' for col in non_numeric_cols})

        # Now dissolve with this aggregation
        metrix_streams_gdf = ripple_reaches_metrix_gdf.dissolve(by='model_id', aggfunc=aggfunc).reset_index()
        metrix_streams_gdf['huc'] = [huc] * len(metrix_streams_gdf)
        # metrix_streams_gdf.loc[:, 'huc'] = huc

        # Move the geometry to the end
        # List all columns except geometry
        first_cols = ['huc', 'collection_id', 'model_id']

        # Make a list of remaining columns, excluding these and geometry
        remaining_cols = [
            col
            for col in metrix_streams_gdf.columns
            if col not in first_cols and col != metrix_streams_gdf.geometry.name
        ]
        # Append geometry column at the end
        new_order = first_cols + remaining_cols + [metrix_streams_gdf.geometry.name]

        # Reorder the GeoDataFrame
        metrix_streams_gdf = metrix_streams_gdf[new_order]
        metrix_streams_gdf = metrix_streams_gdf.replace('', np.nan)
        metrix_streams_gdf = metrix_streams_gdf.dropna(subset=['avg_inundation_overlap'])

        # print(metrix_streams_gdf)
        # Save the gdf
        path_streams_metrix = os.path.join(metrix_dir, ripple_models[rmi], f'streams_metrix_{huc}.gpkg')
        if not os.path.exists(path_streams_metrix):
            metrix_streams_gdf.to_file(path_streams_metrix)

        metrix_streams_conus_gpkg_ls.append(metrix_streams_gdf)

        metrix_streams_df = metrix_streams_gdf.drop(columns=['geometry'])
        metrix_streams_conus_ls.append(metrix_streams_df)

    # Save correlation matrix conus wise in csv format
    model_metrix_corr_df = pd.concat(model_metrix_corr_ls, ignore_index=False)
    model_metrix_corr_path = os.path.join(metrix_dir, 'model_metrix_corr_new.csv')
    model_metrix_corr_df.to_csv(model_metrix_corr_path)

    # Save reaches matrix conus wise in csv format
    metrix_reaches_conus_df = pd.concat(metrix_reaches_conus_ls, axis=0, ignore_index=True)
    metrix_reaches_conus_df = metrix_reaches_conus_df.replace('', np.nan)
    metrix_reaches_conus_df = metrix_reaches_conus_df.dropna(subset=['avg_inundation_overlap'])

    path_metrix_reaches_conus = os.path.join(metrix_dir, 'metrix_reaches_ripple_submodels_conus.csv')
    if not os.path.exists(path_metrix_reaches_conus):
        metrix_reaches_conus_df.to_csv(path_metrix_reaches_conus, index=False)

    # Save stream matrix conus wise in gpkg format
    metrix_streams_conus_gpkg = pd.concat(metrix_streams_conus_gpkg_ls, axis=0, ignore_index=True)
    metrix_streams_conus_gpkg = metrix_streams_conus_gpkg.replace('', np.nan)
    metrix_streams_conus_gpkg = metrix_streams_conus_gpkg.dropna(subset=['avg_inundation_overlap'])

    path_metrix_streams_conus_gpkg = os.path.join(metrix_dir, 'metrix_streams_ripple_submodels_conus.gpkg')
    if not os.path.exists(path_metrix_streams_conus_gpkg):
        metrix_streams_conus_gpkg.to_file(path_metrix_streams_conus_gpkg, index=False)

    # Save stream matrix conus wise in csv formats
    metrix_streams_conus = pd.concat(metrix_streams_conus_ls, axis=0, ignore_index=True)
    metrix_streams_conus = metrix_streams_conus.replace('', np.nan)
    metrix_streams_conus = metrix_streams_conus.dropna(subset=['avg_inundation_overlap'])

    path_metrix_streams_conus = os.path.join(metrix_dir, 'metrix_streams_ripple_submodels_conus.csv')
    if not os.path.exists(path_metrix_streams_conus):
        metrix_streams_conus.to_csv(path_metrix_streams_conus, index=False)

    return log_text


# ***************************************************************
def process_ripple_STREAMS_create_blackList(metrix_dir):

    path_ripple_streams = os.path.join(metrix_dir, 'metrix_streams_ripple_submodels_conus.csv')
    path_ripple_reaches = os.path.join(metrix_dir, 'metrix_reaches_ripple_submodels_conus.csv')

    log_text = ''
    if not os.path.exists(path_ripple_streams):
        log_text += create_ripple_STREAMS_gdf_csv(metrix_dir)

    else:
        log_text += 'Ripple streams matrics csv file already exists ...\n'
        print('Ripple streams matrics csv file already exists ...\n')

    print('Start creating the black list ...\n')
    log_text += 'Start creating the black list ...\n'

    ripple_streams_metrix_df = gpd.read_file(path_ripple_streams)
    # ripple_streams_metrix_df = ripple_streams_metrix_df_geo.drop(columns=['geometry'])  # drop_duplicates()
    ripple_streams_metrix_df = ripple_streams_metrix_df.replace('', np.nan)
    ripple_streams_metrix_df = ripple_streams_metrix_df.dropna(subset=['avg_inundation_overlap'])
    # print(ripple_streams_metrix_df_d[ripple_streams_metrix_df_d['model_id'].isna()])

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
    ripple_streams_metrix_df[numeric_columns] = ripple_streams_metrix_df[numeric_columns].apply(
        pd.to_numeric, errors='coerce'
    )
    # feature_ids with low inundation_overlap metric
    mask1 = (ripple_streams_metrix_df['avg_thalweg_elevation_difference'] >= 100) | (
        ripple_streams_metrix_df['avg_thalweg_elevation_difference'] <= -50
    )
    outlier_fid_thalweg_elev_diff = ripple_streams_metrix_df[mask1]

    mask2 = ripple_streams_metrix_df['avg_inundation_overlap'] <= 0.3
    low_inundation_overlap = ripple_streams_metrix_df[mask2]

    mask3 = (
        (ripple_streams_metrix_df['order_'] <= 3)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] < 0.55)
        & (ripple_streams_metrix_df['avg_r_squared'] < 0.6)
    )
    low_r2_1 = ripple_streams_metrix_df[mask3]

    mask4 = (
        (ripple_streams_metrix_df['order_'] >= 4)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] < 0.5)
        & (ripple_streams_metrix_df['avg_r_squared'] < 0.52)
    )
    low_r2_2 = ripple_streams_metrix_df[mask4]

    # avg_inundation_overlap < = 0.4 and
    # avg_hydraulic_radius_agreement < 0.52
    mask5 = (
        (ripple_streams_metrix_df['order_'] <= 3)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] <= 0.5)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] <= 0.52)
    )
    outlier_fid_hr_fim_1 = ripple_streams_metrix_df[mask5]
    mask6 = (
        (ripple_streams_metrix_df['order_'] >= 4)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] < 0.4)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] < 0.45)
    )
    outlier_fid_hr_fim_2 = ripple_streams_metrix_df[mask6]

    # avg_inundation_overlap < = 0.45 and
    # avg_hydraulic_radius_agreement < 0.5 and
    # avg_thalweg_elevation_difference < -4.2
    mask7 = (
        (ripple_streams_metrix_df['order_'] <= 3)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] <= 0.45)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] <= 0.52)
        & (ripple_streams_metrix_df['avg_thalweg_elevation_difference'] <= -4.2)
    )
    outlier_fid_thalweg_hr_fim1 = ripple_streams_metrix_df[mask7]
    mask8 = (
        (ripple_streams_metrix_df['order_'] >= 4)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] <= 0.40)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] <= 0.5)
        & (ripple_streams_metrix_df['avg_thalweg_elevation_difference'] <= -10)
    )
    outlier_fid_thalweg_hr_fim2 = ripple_streams_metrix_df[mask8]

    # avg_inundation_overlap < = 0.6 and
    # avg_hydraulic_radius_agreement < 0.6 and
    # avg_thalweg_elevation_difference < -10
    mask9 = (
        (ripple_streams_metrix_df['order_'] < 3)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] < 0.55)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] < 0.6)
        & (ripple_streams_metrix_df['avg_thalweg_elevation_difference'] <= -10)
    )
    outlier_fid_thalweg_hr_fim3 = ripple_streams_metrix_df[mask9]

    mask10 = (
        (ripple_streams_metrix_df['order_'] >= 3)
        & (ripple_streams_metrix_df['avg_inundation_overlap'] < 0.51)
        & (ripple_streams_metrix_df['avg_hydraulic_radius_agreement'] < 0.55)
        & (ripple_streams_metrix_df['avg_thalweg_elevation_difference'] <= -45)
    )
    outlier_fid_thalweg_hr_fim4 = ripple_streams_metrix_df[mask10]

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

    path_outlier_streams_conus = os.path.join(metrix_dir, 'outlier_streams_conus.csv')
    outlier_streams_conus_df.to_csv(path_outlier_streams_conus)

    if os.path.exists(path_ripple_reaches):
        ripple_reaches_metrix_df = gpd.read_file(path_ripple_reaches)
        # ripple_reaches_metrix_df = ripple_reaches_metrix_df_geo.drop(
        #     columns=['geometry']
        # )  # drop_duplicates()
        ripple_streams_metrix_df = ripple_streams_metrix_df.replace('', np.nan)
        ripple_reaches_metrix_df = ripple_reaches_metrix_df.dropna(subset=['avg_inundation_overlap'])

    outlier_cols = [
        'collection_id',
        'model_id',
        'feature_id',
        'avg_inundation_overlap',
        'avg_thalweg_elevation_difference',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        # 'avg_spectral_angle'
    ]
    outlier_reaches_conus_df = ripple_reaches_metrix_df.merge(
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
    path_outlier_reaches_conus = os.path.join(metrix_dir, 'outlier_reaches_conus.csv')
    outlier_reaches_conus_df.to_csv(path_outlier_reaches_conus)

    log_text += 'Successfully created a blacklist of the Ripple models from the provided collections. \n'
    print('Successfully created a blacklist of the Ripple models from the provided collections. \n')

    return log_text


# --------------------------------------------------------
# Apply longitudinal dischage adjustment
def apply_ripple_streams_blacklist(metrix_dir, log_file_path):  # bankfull_flows_file,
    """
    Function for processing ripple STREAMS and create a black list of bad models.

    Note: Any failure in here will be logged when it can be but will not abort the Multi-Proc

        Parameters
        ----------
        metrix_dir: str

        Returns
        ----------
        log_text : str
    """
    log_text = ""
    try:
        msg = "Processing ripple STREAMS and create a black list\n"
        log_text += msg
        print(msg)
        log_text += process_ripple_STREAMS_create_blackList(metrix_dir)

    except Exception:
        log_text += "An error has occurred while processing ripple STREAMS"
        log_text += traceback.format_exc()

    try:
        with open(log_file_path, "a") as log_file:
            log_file.write(log_text + '\n')
    except Exception:
        print(f"Error trying to write to the log file of {log_file_path}\n")


# -------------------------------------------------------
def log_create_blacklist(metrix_dir):
    """
    Function for correcting synthetic rating curves using Multi-Proc approach.
    It will correct each branch's SRCs in serial based on the HydroIDs.

        Parameters
        ----------
        metrix-dir : str
            Directory path for saved ripple matrics.

    """
    # Set up log file
    log_file_path = os.path.join(metrix_dir, 'process_ripple_STREAMS' + '.log')
    print(f'Writing progress to log file here: {log_file_path}')
    print('This may take a few minutes...')
    ## Create a time var to log run time
    begin_time = dt.datetime.now(dt.timezone.utc)

    ## Initiate log file
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    with open(log_file_path, "w") as log_file:
        log_file.write('START TIME: ' + str(begin_time) + '\n')
        log_file.write('#########################################################\n\n')

    # Let log_text build up starting here until the bottom.
    log_text = ""

    msg = "Creating a black list of ripple streams\n"
    log_text += msg

    apply_ripple_streams_blacklist(metrix_dir, log_file_path)

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
    metrix-dir : str
        Directory path for saved ripple matrics.

    Sample usage:

    python3 /data/ripple/terrain-agreement-metrics-analysis.py
    -md /outputs/NGWPC-tasks/terrain-agreements/test_pr/metrix_dir1/

    Note: You need to connect to the FIM docker to run this code.

    """
    parser = ArgumentParser(description="Process Ripple Streams and Create a Black List")
    parser.add_argument('-md', '--metrix-dir', help='saved ripple matrics dir', required=True, type=str)
    args = vars(parser.parse_args())

    metrix_dir = args['metrix_dir']

    log_create_blacklist(metrix_dir)


# # ***************************************************************
# def plot_ripple_matrix(metrix_dir, ripple_reaches_metrix_gdf, ripple_model_name):

#     huc = re.search(r'\d+', ripple_model_name).group(0)

#     # Plotting
#     cols = ripple_reaches_metrix_gdf.columns.tolist()[-12:-1]

#     # We want a 10x10 grid for pairs of columns where column i != column j
#     fig, axes = plt.subplots(10, 10, figsize=(20, 20),
#                             sharex=False, sharey=False)
#     colors = ['#FF0000', '#FF8C00', "#615303", '#32CD32', "#0FE7E0", '#0000FF', '#9400D3',
#         "#DC148FB2", "#FD078A", '#D2691E', '#228B22', '#1E90FF', '#9370DB', '#B22222', '#FF6347']
#     for i in range(10):
#         for j in range(10):
#             ax = axes[i, j]

#             # Columns for X and Y axes
#             x_col = cols[i]
#             y_col = cols[j+1]  # shifted by 1 to get other columns if needed

#             # Filter rows where both columns are non-NaN
#             valid = ripple_reaches_metrix_gdf[[x_col, y_col]].dropna()

#             # Scatter plot only if columns are different
#             if x_col != y_col:
#                 ax.scatter(valid[x_col], valid[y_col], s=5, alpha=0.5, color=colors[rmi])
#                 ax.set_xlabel(x_col, fontsize=8)
#                 ax.set_ylabel(y_col, fontsize=8)
#             else:
#                 # ax.text(0.5, 0.5, 'Same column', ha='center', va='center')
#                 # ax.axis('off')
#                 ax.set_visible(False)  # hides axis and everything inside

#             # Adjust tick label font size for clarity
#             ax.tick_params(axis='both', which='major', labelsize=6)

#     fig.suptitle(ripple_model_name, fontsize=20, y=1.02)
#     plt.tight_layout()
#     # plt.show()
#     path_t0_save_fig = os.path.join(metrix_dir, f'scatterplot_metrix_{huc}.png')
#     if not os.path.exists(path_t0_save_fig):
#         plt.savefig(path_t0_save_fig)
