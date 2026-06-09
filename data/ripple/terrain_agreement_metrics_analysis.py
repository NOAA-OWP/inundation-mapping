#!/usr/bin/env python3
# Please Note that in this code each stream is made of one or multiple nwm reaches (or feature-ids)

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


MAX_BRIDGE_REACHES = 1
# metrics_dir = '/outputs/test_blacklist_metrics/collections/'
# out_dir = '/outputs/test_blacklist_metrics/output_metrics_codex_test/'
# ripple_collection_name = 'mip_07140102'


# -----------------------------------------------------------------------------
def retrieve_tiny_unmodeled_ripple_reaches(ripple_gdf, max_bridge_reaches=MAX_BRIDGE_REACHES):
    """
    Return unmodeled reaches that are short topology gaps between modeled reaches.

    """

    if 'feature_id' not in ripple_gdf.columns:
        raise ValueError('ripple_gdf is missing feature_id')

    if 'model_id' not in ripple_gdf.columns:
        raise ValueError('ripple_gdf is missing model_id')

    downstream_col = None
    for col in ['nwm_to_id', 'to', 'to_id', 'NextDownID']:
        if col in ripple_gdf.columns:
            downstream_col = col
            break

    if downstream_col is None:
        raise ValueError(
            'ripple_gdf is missing a downstream pointer column. '
            'Expected one of: nwm_to_id, to, to_id, NextDownID'
        )

    candidates = ripple_gdf.replace('', np.nan).copy()
    candidates = candidates.drop_duplicates(subset='feature_id')
    candidates['has_model_id'] = candidates['model_id'].notna()

    candidate_ids = set(candidates['feature_id'].dropna())
    modeled_ids = set(candidates.loc[candidates['has_model_id'], 'feature_id'].dropna())

    downstream_by_feature_id = candidates.set_index('feature_id')[downstream_col].to_dict()

    bridge_ids = set()
    bridge_upstream_by_id = {}
    bridge_downstream_by_id = {}

    for upstream_modeled_id in modeled_ids:
        gap_ids = []
        seen_ids = {upstream_modeled_id}
        current_id = downstream_by_feature_id.get(upstream_modeled_id)

        for _ in range(max_bridge_reaches):
            if pd.isna(current_id) or current_id not in candidate_ids or current_id in seen_ids:
                break

            seen_ids.add(current_id)

            if current_id in modeled_ids:
                break

            gap_ids.append(current_id)
            next_id = downstream_by_feature_id.get(current_id)

            if pd.isna(next_id):
                break

            if next_id in modeled_ids:
                bridge_ids.update(gap_ids)
                for gap_id in gap_ids:
                    bridge_upstream_by_id[gap_id] = upstream_modeled_id
                    bridge_downstream_by_id[gap_id] = next_id
                break

            current_id = next_id

    bridge_reaches_gdf = candidates.loc[candidates['feature_id'].isin(bridge_ids)].copy()
    bridge_reaches_gdf['bridge_upstream_feature_id'] = bridge_reaches_gdf['feature_id'].map(
        bridge_upstream_by_id
    )
    bridge_reaches_gdf['bridge_downstream_feature_id'] = bridge_reaches_gdf['feature_id'].map(
        bridge_downstream_by_id
    )

    bridge_reaches_gdf = bridge_reaches_gdf.drop(columns=['has_model_id'])

    geom_col = bridge_reaches_gdf.geometry.name
    cols = [col for col in bridge_reaches_gdf.columns if col != geom_col] + [geom_col]

    return bridge_reaches_gdf[cols]


# -----------------------------------------------------------------------------
def merge_nwm_streams_with_ripples(metrics_dir, out_dir, ripple_collection_name):

    src_dir = os.getenv('srcDir')
    if src_dir is None:
        raise EnvironmentError('Environment variable srcDir is not set')

    load_dotenv(os.path.join(src_dir, 'bash_variables.env'))

    pre_clip_huc_dir = os.getenv('pre_clip_huc_dir')
    if pre_clip_huc_dir is None:
        raise EnvironmentError('Environment variable pre_clip_huc_dir is not set')

    huc_match = re.search(r'\d+', ripple_collection_name)
    if huc_match is None:
        raise ValueError(f'Could not determine HUC from ripple collection name: {ripple_collection_name}')

    huc = huc_match.group(0)

    print(f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n')
    log_text = f'Merging nwm_streams with ripple.gpkg for HUC {huc}\n'

    nwm_stream_gpkg = os.path.join(pre_clip_huc_dir, huc, 'nwm_subset_streams.gpkg')
    ripple_gpkg = os.path.join(metrics_dir, ripple_collection_name, 'ripple.gpkg')

    if not os.path.exists(ripple_gpkg):
        msg = f'Ripple GeoPackage does not exist, skipping merge: {ripple_gpkg}\n'
        print(msg)
        log_text += msg
        return log_text

    if not os.path.exists(nwm_stream_gpkg):
        msg = f'NWM streams GeoPackage does not exist, skipping merge: {nwm_stream_gpkg}\n'
        print(msg)
        log_text += msg
        return log_text

    rip_reaches_gdf = gpd.read_file(ripple_gpkg, layer='reaches')
    rip_reaches_gdf = rip_reaches_gdf.rename(columns={'reach_id': 'feature_id'})

    if 'feature_id' not in rip_reaches_gdf.columns:
        msg = f'Ripple reaches layer is missing feature_id/reach_id: {ripple_gpkg}\n'
        print(msg)
        log_text += msg
        return log_text

    with sqlite3.connect(ripple_gpkg) as conn:
        tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)

        if 'processing' not in tables['name'].to_list():
            msg = f'Ripple GeoPackage is missing processing table: {ripple_gpkg}\n'
            print(msg)
            log_text += msg
            return log_text

        rip_process_gdf = pd.read_sql_query('SELECT * FROM processing', conn)

    rip_process_gdf = rip_process_gdf.rename(columns={'reach_id': 'feature_id'})

    required_processing_cols = ['feature_id', 'collection_id', 'model_id']
    missing_processing_cols = [col for col in required_processing_cols if col not in rip_process_gdf.columns]
    if missing_processing_cols:
        msg = f'Processing table is missing columns {missing_processing_cols}: {ripple_gpkg}\n'
        print(msg)
        log_text += msg
        return log_text

    rip_process_gdf = rip_process_gdf[required_processing_cols]

    ripple_gdf = rip_reaches_gdf.merge(rip_process_gdf, on='feature_id', how='left')
    ripple_gdf = ripple_gdf.replace('', np.nan)
    # ripple_gdf = ripple_gdf.dropna(subset=['model_id'])

    tiny_reaches_gdf = retrieve_tiny_unmodeled_ripple_reaches(ripple_gdf, MAX_BRIDGE_REACHES)

    modeled_reaches_gdf = ripple_gdf.dropna(subset=['model_id'])

    ripple_gdf = pd.concat([modeled_reaches_gdf, tiny_reaches_gdf], ignore_index=True)
    ripple_gdf = ripple_gdf.drop_duplicates(subset=['feature_id'])

    nwms_gdf = gpd.read_file(nwm_stream_gpkg)
    nwms_gdf = nwms_gdf.rename(columns={'ID': 'feature_id'})

    required_nwm_cols = ['feature_id', 'order_']
    missing_nwm_cols = [col for col in required_nwm_cols if col not in nwms_gdf.columns]
    if missing_nwm_cols:
        msg = f'NWM streams GeoPackage is missing columns {missing_nwm_cols}: {nwm_stream_gpkg}\n'
        print(msg)
        log_text += msg
        return log_text

    nwms_gdf = nwms_gdf[required_nwm_cols]

    ripple_reaches_gdf = ripple_gdf.merge(nwms_gdf, on='feature_id', how='left')

    ripple_reaches_gdf['is_blacklisted'] = False
    ripple_reaches_gdf['is_valid'] = True

    geom_col = ripple_reaches_gdf.geometry.name
    cols = [col for col in ripple_reaches_gdf.columns if col != geom_col] + [geom_col]
    ripple_reaches_gdf = ripple_reaches_gdf[cols]

    ripple_reaches_gdf['collection_id'] = np.where(
        ripple_reaches_gdf['model_id'].notna(), ripple_collection_name, None
    )

    huc_out_folder = os.path.join(out_dir, ripple_collection_name)
    os.makedirs(huc_out_folder, exist_ok=True)

    path_ripple_reaches = os.path.join(huc_out_folder, f'ripple_reaches_order_sourcemodels_{huc}.gpkg')

    if not os.path.exists(path_ripple_reaches):
        ripple_reaches_gdf.to_file(path_ripple_reaches, driver='GPKG')
    else:
        msg = f'Ripple reaches GeoPackage already exists, skipping write: {path_ripple_reaches}\n'
        print(msg)
        log_text += msg

    return log_text


# -----------------------------------------------------------------------------
def merge_ripple_reaches_sourcemodels_with_metrics_db(metrics_dir, out_dir, ripple_collection_name):
    huc_match = re.search(r'\d+', ripple_collection_name)
    if huc_match is None:
        raise ValueError(f'Could not determine HUC from ripple collection name: {ripple_collection_name}')

    huc = huc_match.group(0)

    path_ripple_collection_out = os.path.join(out_dir, ripple_collection_name)
    path_ripple_reaches = os.path.join(
        path_ripple_collection_out, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
    )

    log_text = ''

    if not os.path.exists(path_ripple_reaches):
        log_text += merge_nwm_streams_with_ripples(metrics_dir, out_dir, ripple_collection_name)

    print(f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n')
    log_text += f'Merging nwm_streams_ripple.gpkg with metrics database for HUC {huc}\n'

    dataset_dir = os.path.join(metrics_dir, ripple_collection_name)
    db_paths = list(Path(dataset_dir).rglob('*.db'))

    if len(db_paths) == 0:
        msg = f'{ripple_collection_name} does not have any metrics database\n'
        print(msg)
        log_text += msg
        return log_text

    model_metrics_ls = []

    for db_path in db_paths:
        feature_id = Path(db_path).stem.split('.')[0]

        parts = db_path.parts
        if 'collections' in parts:
            idx = parts.index('collections')
            relative_db_path = str(Path(*parts[idx + 1 : -1]))
        else:
            relative_db_path = str(Path(db_path).parent)

        with sqlite3.connect(db_path) as conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)

            if 'model_metrics' not in tables['name'].to_list():
                msg = f'Skipping metrics database without model_metrics table: {db_path}\n'
                print(msg)
                log_text += msg
                continue

            mm_df = pd.read_sql_query('SELECT * FROM model_metrics', conn)

        mm_df['feature_id'] = int(feature_id)
        mm_df['db_path'] = relative_db_path
        model_metrics_ls.append(mm_df)

    if len(model_metrics_ls) == 0:
        msg = f'{ripple_collection_name} does not have any readable model_metrics tables\n'
        print(msg)
        log_text += msg
        return log_text

    model_metrics_df = pd.concat(model_metrics_ls, ignore_index=True)

    path_metrics_table_huc = os.path.join(
        path_ripple_collection_out, f'ripple_reaches_sourcemodels_metrics_{huc}.csv'
    )

    if not os.path.exists(path_metrics_table_huc):
        model_metrics_df.to_csv(path_metrics_table_huc, index=False)
    else:
        msg = f'Metrics table already exists, skipping CSV write: {path_metrics_table_huc}\n'
        print(msg)
        log_text += msg

    if not os.path.exists(path_ripple_reaches):
        msg = f'Ripple reaches file does not exist, skipping metrics merge: {path_ripple_reaches}\n'
        print(msg)
        log_text += msg
        return log_text

    ripple_reaches_submod_gdf = gpd.read_file(path_ripple_reaches).drop_duplicates()

    ripple_reaches_metrics_gdf = ripple_reaches_submod_gdf.merge(
        model_metrics_df, on='feature_id', how='left'
    )

    geom_col = ripple_reaches_metrics_gdf.geometry.name
    cols = [col for col in ripple_reaches_metrics_gdf.columns if col != geom_col] + [geom_col]
    ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf[cols]

    if 'avg_inundation_overlap' not in ripple_reaches_metrics_gdf.columns:
        msg = 'Column avg_inundation_overlap is missing, skipping metrics geopackage write\n'
        print(msg)
        log_text += msg
        return log_text

    ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.replace('', np.nan)
    # ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.dropna(subset=['avg_inundation_overlap'])

    path_ripple_reaches_metrics = os.path.join(
        path_ripple_collection_out, f'ripple_reaches_order_source_models_metrics_{huc}.gpkg'
    )

    if not os.path.exists(path_ripple_reaches_metrics):
        ripple_reaches_metrics_gdf.to_file(path_ripple_reaches_metrics)
    else:
        msg = f'Metrics geopackage already exists, skipping write: {path_ripple_reaches_metrics}\n'
        print(msg)
        log_text += msg

    return log_text


# -----------------------------------------------------------------------------
def create_ripple_STREAMS_gdf_csv(metrics_dir, out_dir):

    # Please Note that each stream is made of one or multiple nwm reaches (or feature-ids)
    def log(message):
        print(message)
        return f'{message}\n'

    if not os.path.isdir(metrics_dir):
        raise FileNotFoundError(f'Metrics directory does not exist: {metrics_dir}')

    os.makedirs(out_dir, exist_ok=True)

    ripple_collections = [d for d in os.listdir(metrics_dir) if os.path.isdir(os.path.join(metrics_dir, d))]
    ripple_collections.sort()

    log_text = log(f'{len(ripple_collections)} ripple collections have been found to analyze.')

    metrics_streams_conus_ls = []
    metrics_streams_conus_gpkg_ls = []
    metrics_reaches_conus_ls = []
    all_sourcemodels_reaches_ls = []
    all_sourcemodels_reaches_gpkg_ls = []
    for ripple_collection in ripple_collections:
        try:
            huc_match = re.search(r'\d+', ripple_collection)
            if huc_match is None:
                log_text += log(f'Skipping collection without HUC digits: {ripple_collection}')
                continue

            huc = huc_match.group(0)
            log_text += log(f'Start analyzing ripple collection for HUC {huc}')

            # Read metrics geopackage
            path_ripple_collection_out = os.path.join(out_dir, ripple_collection)
            path_ripple_reaches = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_order_source_models_metrics_{huc}.gpkg'
            )

            if not os.path.exists(path_ripple_reaches):
                log_text += merge_ripple_reaches_sourcemodels_with_metrics_db(
                    metrics_dir, out_dir, ripple_collection
                )

            path_sourcemodels_gpkg = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_order_sourcemodels_{huc}.gpkg'
            )
            path_metrics_csv = os.path.join(
                path_ripple_collection_out, f'ripple_reaches_sourcemodels_metrics_{huc}.csv'
            )

            if os.path.exists(path_sourcemodels_gpkg):
                sourcemodels_gdf = gpd.read_file(path_sourcemodels_gpkg)

                if 'is_blacklisted' not in sourcemodels_gdf.columns:
                    sourcemodels_gdf['is_blacklisted'] = False

                if 'is_valid' not in sourcemodels_gdf.columns:
                    sourcemodels_gdf['is_valid'] = True

                sourcemodels_gdf['huc'] = huc

                if os.path.exists(path_metrics_csv):
                    metric_df = pd.read_csv(path_metrics_csv)

                    if 'db_path' in metric_df.columns and 'feature_id' in metric_df.columns:
                        db_path_df = metric_df[['feature_id', 'db_path']].drop_duplicates()

                        sourcemodels_gdf['feature_id'] = pd.to_numeric(
                            sourcemodels_gdf['feature_id'], errors='coerce'
                        ).astype('Int64')

                        db_path_df['feature_id'] = pd.to_numeric(
                            db_path_df['feature_id'], errors='coerce'
                        ).astype('Int64')

                        sourcemodels_gdf = sourcemodels_gdf.merge(db_path_df, on='feature_id', how='left')

                    elif 'db_path' not in sourcemodels_gdf.columns:
                        sourcemodels_gdf['db_path'] = None

                elif 'db_path' not in sourcemodels_gdf.columns:
                    sourcemodels_gdf['db_path'] = None

                sourcemodels_df = sourcemodels_gdf.drop(columns=['geometry'], errors='ignore')
                all_sourcemodels_reaches_ls.append(sourcemodels_df)
                all_sourcemodels_reaches_gpkg_ls.append(sourcemodels_gdf)

            if not os.path.exists(path_ripple_reaches):
                log_text += log(f'Metrics reaches GeoPackage does not exist, skipping: {path_ripple_reaches}')
                continue

            ripple_reaches_metrics_gdf = gpd.read_file(path_ripple_reaches).drop_duplicates()
            ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.replace('', np.nan)

            required_cols = ['model_id', 'avg_inundation_overlap']
            missing_cols = [col for col in required_cols if col not in ripple_reaches_metrics_gdf.columns]
            if missing_cols:
                log_text += log(f'Skipping {ripple_collection}; missing columns: {missing_cols}')
                continue

            # ripple_reaches_metrics_gdf = ripple_reaches_metrics_gdf.dropna(subset=['avg_inundation_overlap'])

            if len(ripple_reaches_metrics_gdf) == 0:
                log_text += log(f'Skipping {ripple_collection}; no valid metric rows remain')
                continue

            # Remove geometry column if present (geometry handled automatically)
            ripple_reaches_metrics_df = ripple_reaches_metrics_gdf.drop(columns=['geometry'], errors='ignore')
            metrics_reaches_conus_ls.append(ripple_reaches_metrics_df)

            # Identify numeric columns
            numeric_cols = ripple_reaches_metrics_gdf.select_dtypes(include='number').columns.tolist()

            # Specify which numeric columns get 'max'
            max_cols = [
                col
                for col in ['feature_id', 'nwm_to_id', 'order_']
                if col in ripple_reaches_metrics_gdf.columns
            ]

            # Columns to average = numeric columns excluding max_cols
            mean_cols = [col for col in numeric_cols if col not in max_cols]

            # Identify non-numeric columns (excluding geometry)
            non_numeric_cols = [col for col in ['collection_id'] if col in ripple_reaches_metrics_gdf.columns]

            # Build the aggfunc dictionary
            aggfunc = {col: 'max' for col in max_cols}
            aggfunc.update({col: 'mean' for col in mean_cols})
            aggfunc.update({col: 'first' for col in non_numeric_cols})

            metrics_streams_gdf = ripple_reaches_metrics_gdf.dissolve(
                by='model_id', aggfunc=aggfunc
            ).reset_index()
            metrics_streams_gdf['huc'] = huc

            # Move the geometry to the end
            # List all columns except geometry
            first_cols = [
                col for col in ['huc', 'collection_id', 'model_id'] if col in metrics_streams_gdf.columns
            ]
            geom_col = metrics_streams_gdf.geometry.name
            remaining_cols = [
                col for col in metrics_streams_gdf.columns if col not in first_cols and col != geom_col
            ]
            metrics_streams_gdf = metrics_streams_gdf[first_cols + remaining_cols + [geom_col]]

            metrics_streams_gdf = metrics_streams_gdf.replace('', np.nan)
            # metrics_streams_gdf = metrics_streams_gdf.dropna(subset=['avg_inundation_overlap'])

            path_streams_metrics = os.path.join(path_ripple_collection_out, f'streams_metrics_{huc}.gpkg')
            if not os.path.exists(path_streams_metrics):
                metrics_streams_gdf.to_file(path_streams_metrics)
            else:
                log_text += log(
                    f'Stream metrics GeoPackage already exists, skipping write: {path_streams_metrics}'
                )

            metrics_streams_conus_gpkg_ls.append(metrics_streams_gdf)
            metrics_streams_conus_ls.append(metrics_streams_gdf.drop(columns=['geometry'], errors='ignore'))

        except Exception as e:
            error_msg = f'Error processing folder {ripple_collection}: {str(e)}'
            print(error_msg)
            print(traceback.format_exc())
            log_text += f'{error_msg}\n'
            continue

    # Save reaches matrix conus-wise in csv format
    if metrics_reaches_conus_ls:

        metrics_reaches_conus_df = pd.concat(metrics_reaches_conus_ls, ignore_index=True)
        metrics_reaches_conus_df = metrics_reaches_conus_df.replace('', np.nan)
        # metrics_reaches_conus_df = metrics_reaches_conus_df.dropna(subset=['avg_inundation_overlap'])

        path_metrics_reaches_conus = os.path.join(out_dir, 'metrics_reaches_ripple_submodels_conus.csv')
        if not os.path.exists(path_metrics_reaches_conus):
            metrics_reaches_conus_df.to_csv(path_metrics_reaches_conus, index=False)

    else:
        log_text += log('No reach metrics were created.')

    if metrics_streams_conus_gpkg_ls:
        metrics_streams_conus_gpkg = pd.concat(metrics_streams_conus_gpkg_ls, ignore_index=True)
        metrics_streams_conus_gpkg = metrics_streams_conus_gpkg.replace('', np.nan)
        # metrics_streams_conus_gpkg = metrics_streams_conus_gpkg.dropna(subset=['avg_inundation_overlap'])

        path_metrics_streams_conus_gpkg = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.gpkg')
        if not os.path.exists(path_metrics_streams_conus_gpkg):
            metrics_streams_conus_gpkg.to_file(path_metrics_streams_conus_gpkg)
    else:
        log_text += log('No stream metrics GeoPackages were created.')

    if metrics_streams_conus_ls:
        metrics_streams_conus = pd.concat(metrics_streams_conus_ls, ignore_index=True)
        metrics_streams_conus = metrics_streams_conus.replace('', np.nan)
        # metrics_streams_conus = metrics_streams_conus.dropna(subset=['avg_inundation_overlap'])

        path_metrics_streams_conus = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.csv')
        if not os.path.exists(path_metrics_streams_conus):
            metrics_streams_conus.to_csv(path_metrics_streams_conus, index=False)
    else:
        log_text += log('No stream metrics CSV was created.')

    if all_sourcemodels_reaches_ls:
        all_sourcemodels_conus_df = pd.concat(all_sourcemodels_reaches_ls, ignore_index=True)
        path_all_sourcemodels_conus = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.csv')
        all_sourcemodels_conus_df.to_csv(path_all_sourcemodels_conus, index=False)

    if all_sourcemodels_reaches_gpkg_ls:
        all_sourcemodels_conus_gdf = pd.concat(all_sourcemodels_reaches_gpkg_ls, ignore_index=True)
        path_all_sourcemodels_conus_gpkg = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.gpkg')
        all_sourcemodels_conus_gdf.to_file(path_all_sourcemodels_conus_gpkg, driver='GPKG')

    return log_text


def process_ripple_STREAMS_create_blackList(metrics_dir, out_dir):

    def log(message):
        print(message)
        return f'{message}\n'

    # Please Note that each stream is made of one or multiple nwm reaches (or feature-ids)
    path_ripple_streams = os.path.join(out_dir, 'metrics_streams_ripple_submodels_conus.csv')
    path_ripple_reaches = os.path.join(out_dir, 'metrics_reaches_ripple_submodels_conus.csv')

    log_text = ''

    if not os.path.exists(path_ripple_streams) or not os.path.exists(path_ripple_reaches):
        log_text += create_ripple_STREAMS_gdf_csv(metrics_dir, out_dir)
    else:
        log_text += log('Ripple streams and reaches metrics CSV files already exist.')

    if not os.path.exists(path_ripple_streams):
        msg = f'Ripple streams metrics CSV does not exist: {path_ripple_streams}'
        log_text += log(msg)
        return log_text

    log_text += log('Start creating the black list ...')

    ripple_streams_metrics_df = pd.read_csv(path_ripple_streams)
    ripple_streams_metrics_df = ripple_streams_metrics_df.replace('', np.nan)

    required_columns = [
        'collection_id',
        'model_id',
        'feature_id',
        'order_',
        'avg_inundation_overlap',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        'avg_thalweg_elevation_difference',
    ]
    missing_columns = [col for col in required_columns if col not in ripple_streams_metrics_df.columns]
    if missing_columns:
        msg = f'Cannot create blacklist; stream metrics CSV is missing columns: {missing_columns}'
        log_text += log(msg)
        return log_text

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
    numeric_columns = [col for col in numeric_columns if col in ripple_streams_metrics_df.columns]
    ripple_streams_metrics_df[numeric_columns] = ripple_streams_metrics_df[numeric_columns].apply(
        pd.to_numeric, errors='coerce'
    )

    # Missing metrics cannot satisfy blacklist thresholds, so keep them in source data
    # but evaluate only rows with enough metric data.
    streams_for_blacklist = ripple_streams_metrics_df.dropna(subset=['avg_inundation_overlap'])

    outlier_frames = [
        streams_for_blacklist[
            (streams_for_blacklist['avg_thalweg_elevation_difference'] >= 100)
            | (streams_for_blacklist['avg_thalweg_elevation_difference'] <= -50)
        ],
        streams_for_blacklist[streams_for_blacklist['avg_inundation_overlap'] <= 0.3],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] <= 3)
            & (streams_for_blacklist['avg_inundation_overlap'] < 0.55)
            & (streams_for_blacklist['avg_r_squared'] < 0.6)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] >= 4)
            & (streams_for_blacklist['avg_inundation_overlap'] < 0.5)
            & (streams_for_blacklist['avg_r_squared'] < 0.52)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] <= 3)
            & (streams_for_blacklist['avg_inundation_overlap'] <= 0.5)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] <= 0.52)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] >= 4)
            & (streams_for_blacklist['avg_inundation_overlap'] < 0.4)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] < 0.45)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] <= 3)
            & (streams_for_blacklist['avg_inundation_overlap'] <= 0.45)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] <= 0.52)
            & (streams_for_blacklist['avg_thalweg_elevation_difference'] <= -4.2)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] >= 4)
            & (streams_for_blacklist['avg_inundation_overlap'] <= 0.40)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] <= 0.5)
            & (streams_for_blacklist['avg_thalweg_elevation_difference'] <= -10)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] < 3)
            & (streams_for_blacklist['avg_inundation_overlap'] < 0.55)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] < 0.6)
            & (streams_for_blacklist['avg_thalweg_elevation_difference'] <= -10)
        ],
        streams_for_blacklist[
            (streams_for_blacklist['order_'] >= 3)
            & (streams_for_blacklist['avg_inundation_overlap'] < 0.51)
            & (streams_for_blacklist['avg_hydraulic_radius_agreement'] < 0.55)
            & (streams_for_blacklist['avg_thalweg_elevation_difference'] <= -45)
        ],
    ]

    outlier_streams_conus_df = pd.concat(outlier_frames, ignore_index=True).drop_duplicates()

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
    cols_rearranged = [col for col in col_to_front if col in outlier_streams_conus_df.columns] + [
        col for col in outlier_streams_conus_df.columns if col not in col_to_front
    ]
    outlier_streams_conus_df = outlier_streams_conus_df[cols_rearranged]

    num_outlier_streams_conus = len(outlier_streams_conus_df)
    log_text += log(f'Number of the outlier Ripple models is {num_outlier_streams_conus}')

    path_outlier_streams_conus = os.path.join(out_dir, 'outlier_streams_conus.csv')
    outlier_streams_conus_df.to_csv(path_outlier_streams_conus, index=False)

    # ** Expanding outlier streams to outlier reaches **
    if not os.path.exists(path_ripple_reaches):
        msg = f'Ripple reaches metrics CSV does not exist: {path_ripple_reaches}'
        log_text += log(msg)
        return log_text

    ripple_reaches_metrics_df = pd.read_csv(path_ripple_reaches)
    ripple_reaches_metrics_df = ripple_reaches_metrics_df.replace('', np.nan)

    stream_outlier_cols = [
        'collection_id',
        'model_id',
        'feature_id',
        'avg_inundation_overlap',
        'avg_thalweg_elevation_difference',
        'avg_hydraulic_radius_agreement',
        'avg_r_squared',
        'huc',
    ]
    stream_outlier_cols = [col for col in stream_outlier_cols if col in outlier_streams_conus_df.columns]

    outlier_streams_for_merge = outlier_streams_conus_df[stream_outlier_cols].rename(
        columns={
            'feature_id': 'stream_feature_id',
            'avg_inundation_overlap': 'stream_avg_inundation_overlap',
            'avg_thalweg_elevation_difference': 'stream_avg_thalweg_elevation_difference',
            'avg_hydraulic_radius_agreement': 'stream_avg_hydraulic_radius_agreement',
            'avg_r_squared': 'stream_avg_r_squared',
        }
    )

    outlier_reaches_conus_df = ripple_reaches_metrics_df.merge(
        outlier_streams_for_merge, on=['collection_id', 'model_id'], how='inner'
    ).drop_duplicates()

    outlier_reaches_conus_df = outlier_reaches_conus_df.rename(
        columns={
            'avg_inundation_overlap': 'inundation_overlap',
            'avg_thalweg_elevation_difference': 'thalweg_elevation_difference',
            'avg_hydraulic_radius_agreement': 'hydraulic_radius_agreement',
            'avg_r_squared': 'r_squared',
            'stream_avg_inundation_overlap': 'avg_inundation_overlap',
            'stream_avg_thalweg_elevation_difference': 'avg_thalweg_elevation_difference',
            'stream_avg_hydraulic_radius_agreement': 'avg_hydraulic_radius_agreement',
            'stream_avg_r_squared': 'avg_r_squared',
        }
    )

    path_outlier_reaches_conus = os.path.join(out_dir, 'outlier_reaches_conus.csv')
    outlier_reaches_conus_df.to_csv(path_outlier_reaches_conus, index=False)

    path_all_sourcemodels_conus = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.csv')
    if os.path.exists(path_all_sourcemodels_conus):
        log_text += log('Creating master CONUS whitelist csv...')

        all_sourcemodels_conus_df = pd.read_csv(path_all_sourcemodels_conus)

        bad_features = outlier_reaches_conus_df[['collection_id', 'feature_id']].drop_duplicates()
        bad_features['is_bad'] = True

        bad_features['feature_id'] = pd.to_numeric(bad_features['feature_id'], errors='coerce').astype(
            'Int64'
        )
        all_sourcemodels_conus_df['feature_id'] = pd.to_numeric(
            all_sourcemodels_conus_df['feature_id'], errors='coerce'
        ).astype('Int64')

        merged_all = all_sourcemodels_conus_df.merge(
            bad_features, on=['collection_id', 'feature_id'], how='left'
        )

        merged_all['is_blacklisted'] = merged_all['is_bad'].fillna(False)
        merged_all['is_valid'] = ~merged_all['is_blacklisted']
        merged_all = merged_all.drop(columns=['is_bad'])

        path_master_whitelist_conus = os.path.join(out_dir, 'ripple_feature_id_whitelist_conus.csv')
        merged_all.to_csv(path_master_whitelist_conus, index=False)

        # Create a gpkg of whitelist reaches and metrics
        path_all_sourcemodels_conus_gpkg = os.path.join(out_dir, 'all_reaches_sourcemodels_conus.gpkg')

        if os.path.exists(path_all_sourcemodels_conus_gpkg):
            all_sourcemodels_conus_gdf = gpd.read_file(path_all_sourcemodels_conus_gpkg)

            all_sourcemodels_conus_gdf['feature_id'] = pd.to_numeric(
                all_sourcemodels_conus_gdf['feature_id'], errors='coerce'
            ).astype('Int64')

            whitelist_cols = ['collection_id', 'feature_id', 'is_blacklisted', 'is_valid']

            whitelist_gdf = all_sourcemodels_conus_gdf.drop(
                columns=['is_blacklisted', 'is_valid'], errors='ignore'
            ).merge(merged_all[whitelist_cols], on=['collection_id', 'feature_id'], how='left')

            whitelist_gdf['is_blacklisted'] = whitelist_gdf['is_blacklisted'].fillna(False)
            whitelist_gdf['is_valid'] = ~whitelist_gdf['is_blacklisted']

            path_master_whitelist_conus_gpkg = os.path.join(out_dir, 'ripple_feature_id_whitelist_conus.gpkg')
            whitelist_gdf.to_file(path_master_whitelist_conus_gpkg, driver='GPKG')
        else:
            log_text += log(
                f'All source models CONUS GeoPackage does not exist: {path_all_sourcemodels_conus_gpkg}'
            )
    else:
        log_text += log(f'All source models CSV does not exist: {path_all_sourcemodels_conus}')

    log_text += log('Successfully created a blacklist of the Ripple models from the provided collections.')

    return log_text


# -----------------------------------------------------------------------------
# Apply ripple_streams_blacklist function on metrics_dir
def apply_ripple_streams_blacklist(metrics_dir, out_dir, log_file_path):
    """
    Process Ripple stream metrics and create blacklist/whitelist outputs.

    Parameters
    ----------
    metrics_dir : str
        Directory containing Ripple collection metrics.
    out_dir : str
        Directory where output CSVs, GeoPackages, and logs are written.
    log_file_path : str
        Path to the run log file.

    Returns
    -------
    str
        Log text generated during processing.
    """
    log_text = ''

    try:
        msg = 'Processing Ripple STREAMS and creating blacklist\n'
        print(msg)
        log_text += msg

        log_text += process_ripple_STREAMS_create_blackList(metrics_dir, out_dir)

    except Exception:
        error_msg = 'An error occurred while processing Ripple STREAMS\n'
        print(error_msg)
        log_text += error_msg
        log_text += traceback.format_exc()

    try:
        os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
        with open(log_file_path, 'a') as log_file:
            log_file.write(log_text)
            if not log_text.endswith('\n'):
                log_file.write('\n')
    except Exception:
        print(f'Error trying to write to the log file: {log_file_path}\n')
        print(traceback.format_exc())

    return log_text


# -----------------------------------------------------------------------------
def log_create_blacklist(metrics_dir, out_dir):
    """
    Create a timestamped log file and run the Ripple stream blacklist workflow.

    Parameters
    ----------
    metrics_dir : str
        Directory containing Ripple collection metrics.
    out_dir : str
        Directory where output CSVs, GeoPackages, and logs are written.

    Returns
    -------
    str
        Full log text for the run.
    """
    print('This may take a few minutes...')

    os.makedirs(out_dir, exist_ok=True)

    begin_time = dt.datetime.now(dt.timezone.utc)
    timestamp = begin_time.strftime('%Y%m%d_%H%M%S')
    log_file_name = f'process_ripple_STREAMS_{timestamp}.log'
    log_file_path = os.path.join(out_dir, log_file_name)

    print(f'Writing progress to log file here: {log_file_path}')

    log_text = ''
    log_text += f'START TIME: {begin_time}\n'
    log_text += '--------------------------------------------------\n\n'
    log_text += 'Creating a blacklist of Ripple streams\n'

    with open(log_file_path, 'w') as log_file:
        log_file.write(log_text)

    log_text += apply_ripple_streams_blacklist(metrics_dir, out_dir, log_file_path)

    end_time = dt.datetime.now(dt.timezone.utc)
    total_run_time = end_time - begin_time

    final_log_text = ''
    final_log_text += f'END TIME: {end_time}\n'
    final_log_text += f'TOTAL RUN TIME: {str(total_run_time).split(".")[0]}\n'

    print(final_log_text)
    log_text += final_log_text

    with open(log_file_path, 'a') as log_file:
        log_file.write(final_log_text)

    return log_text


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
