
import argparse
import os
from os.path import join
import glob
import numpy as np

import geopandas as gpd
import pandas as pd
import datetime


optz_metrics_dir = '/outputs/optz_final_all/' # roughness_optz_v62
OPTZ_METRICS_DIR = optz_metrics_dir
pre_clip_huc8_dir = '/data/inputs/pre_clip_huc8/20260615/'
PRE_CLIP_HUC8_DIR = pre_clip_huc8_dir
RECURRENCE_CLUSTER_FILENAME = 'recurrence_flows_nwm_v3_CONUS_100_interval_added_clusters.csv'
OPTZ_MANNINGS_FILENAME = 'optz_mannings_v6_1_1.csv'
OPTZ_MANNINGS_OUTPUT_FILENAME = 'optz_mannings_v6_2.csv'
OHIO_ALLEGENY_MONONGAHELA_RIVERS_FILENAME = 'ohio_allegeny_monongahela_rivers_fids_mannings_n.csv'
CHANNEL_N_OHIO_ALLEGENY_MONONGAHELA = 0.012
OVERBANK_N_OHIO_ALLEGENY_MONONGAHELA = 0.0482
CHANNEL_N_NAN_FILL = 0.06
OVERBANK_N_NAN_FILL = 0.12
LOSS_DEVIATION_THRESHOLD = 15
CH_N_VALID_THRESHOLD = (0.011, 0.09)
OB_N_VALID_THRESHOLD = (0.040, 0.17)


# *****************************************************************************
def get_optimized_hucs(optz_metrics_dir):
    optz_csv_paths = glob.glob(join(optz_metrics_dir, 'optz_iteration_metrics_*.csv'))

    if not optz_csv_paths:
        optz_csv_paths = glob.glob(join(optz_metrics_dir, '*', 'optz_iteration_metrics_*.csv'))

    if not optz_csv_paths:
        raise FileNotFoundError(
            f"No optz_iteration_metrics_*.csv files found in {optz_metrics_dir} or its child directories"
        )

    hucs = {
        os.path.basename(path).replace('optz_iteration_metrics_', '').replace('.csv', '')
        for path in optz_csv_paths
    }

    return sorted(hucs)


# *****************************************************************************
def _read_stream_feature_ids(stream_path):
    stream_df = gpd.read_file(stream_path, ignore_geometry=True)

    if 'ID' not in stream_df.columns:
        raise ValueError(f"{stream_path} is missing required column: ID")

    if 'order_' not in stream_df.columns:
        raise ValueError(f"{stream_path} is missing required column: order_")

    return (
        stream_df[['ID', 'order_']]
        .rename(columns={'ID': 'feature_id'})
        .drop_duplicates(subset=['feature_id'])
    )


# *****************************************************************************
def _read_recurrence_flow_clusters(cluster_csv_path, feature_ids=None):
    """ reads every chunksize=500000 rows of feature-ids and process them to save time"""

    required_cols = ['feature_id', 'runoff_cluster_idx', 'latitude', 'longitude']
    feature_id_set = None
    if feature_ids is not None:
        feature_id_set = set(pd.Series(feature_ids).dropna().astype('int64'))

    cluster_chunks = []
    for chunk in pd.read_csv(
        cluster_csv_path,
        usecols=required_cols,
        dtype={
            'feature_id': 'int64',
            'runoff_cluster_idx': 'Int64',
            'latitude': float,
            'longitude': float,
        },
        chunksize=500000,
    ):
        if feature_id_set is not None:
            chunk = chunk.loc[chunk['feature_id'].isin(feature_id_set)]
        if not chunk.empty:
            cluster_chunks.append(chunk)

    if not cluster_chunks:
        return pd.DataFrame(
            {
                'feature_id': pd.Series(dtype='int64'),
                'runoff_cluster_idx': pd.Series(dtype='Int64'),
                'latitude': pd.Series(dtype=float),
                'longitude': pd.Series(dtype=float),
            }
        )

    cluster_df = pd.concat(cluster_chunks, ignore_index=True).drop_duplicates(subset=['feature_id'])
    return cluster_df


# *****************************************************************************
def _get_pre_clip_hucs(pre_clip_huc8_dir):
    hucs = [
        os.path.basename(path)
        for path in glob.glob(join(pre_clip_huc8_dir, '*'))
        if os.path.isdir(path)
    ]

    if not hucs:
        raise FileNotFoundError(f"No HUC directories found in {pre_clip_huc8_dir}")

    return sorted(hucs)


# *****************************************************************************
def _read_huc_feature_df(hucs, pre_clip_huc8_dir, feature_ids=None):
    """ reads huc feature-ids and stream orders from pre_clip_huc8_dir"""

    huc_feature_rows = []
    missing_hucs = []
    feature_id_set = None
    if feature_ids is not None:
        feature_id_set = set(pd.Series(feature_ids).dropna().astype('int64'))

    for huc in hucs:
        stream_path = join(pre_clip_huc8_dir, huc, 'nwm_subset_streams.gpkg')

        if not os.path.exists(stream_path):
            missing_hucs.append(huc)
            continue

        huc_streams = _read_stream_feature_ids(stream_path)
        huc_streams['feature_id'] = huc_streams['feature_id'].astype('int64')

        if feature_id_set is not None:
            huc_streams = huc_streams.loc[huc_streams['feature_id'].isin(feature_id_set)]

        if huc_streams.empty:
            continue

        huc_streams['huc'] = huc
        huc_feature_rows.append(huc_streams[['huc', 'feature_id', 'order_']])

    if missing_hucs:
        raise FileNotFoundError(
            f"Missing nwm_subset_streams.gpkg for {len(missing_hucs)} HUC(s): "
            f"{', '.join(missing_hucs[:10])}"
        )

    if not huc_feature_rows:
        return pd.DataFrame(
            {
                'huc': pd.Series(dtype=str),
                'feature_id': pd.Series(dtype='int64'),
                'order_': pd.Series(dtype='int64'),
            }
        )

    huc_feature_df = pd.concat(huc_feature_rows, ignore_index=True)
    huc_feature_df['huc'] = huc_feature_df['huc'].astype(str)
    huc_feature_df['feature_id'] = huc_feature_df['feature_id'].astype('int64')
    huc_feature_df = huc_feature_df.drop_duplicates(subset=['huc', 'feature_id'])
    huc_feature_df['order_'] = pd.to_numeric(huc_feature_df['order_'], errors='raise').astype('int64')

    return huc_feature_df


# *****************************************************************************
def _resolve_duplicate_feature_hucs(huc_feature_cluster_df, pre_clip_huc8_dir):

    duplicate_feature_ids = huc_feature_cluster_df.loc[
        huc_feature_cluster_df.duplicated('feature_id', keep=False), 'feature_id'
    ].drop_duplicates()

    if duplicate_feature_ids.empty:
        return huc_feature_cluster_df

    duplicate_feature_id_set = set(duplicate_feature_ids.astype(int))
    duplicate_candidates = huc_feature_cluster_df.loc[
        huc_feature_cluster_df['feature_id'].isin(duplicate_feature_id_set)
    ].copy()
    wbd_check_rows = []

    for huc, huc_duplicates in duplicate_candidates.groupby('huc'):
        wbd_path = join(pre_clip_huc8_dir, huc, 'wbd.gpkg')

        if not os.path.exists(wbd_path):
            continue

        huc_duplicates = huc_duplicates.dropna(subset=['longitude', 'latitude']).copy()
        if huc_duplicates.empty:
            continue

        wbd = gpd.read_file(wbd_path)
        if wbd.crs is not None and str(wbd.crs).upper() != 'EPSG:4326':
            wbd = wbd.to_crs('EPSG:4326')

        wbd_geom = wbd.geometry.union_all() if hasattr(wbd.geometry, 'union_all') else wbd.geometry.unary_union
        points = gpd.GeoSeries(
            gpd.points_from_xy(huc_duplicates['longitude'], huc_duplicates['latitude']),
            crs='EPSG:4326',
        )
        huc_duplicates['point_in_wbd'] = points.intersects(wbd_geom).to_numpy()
        wbd_check_rows.append(huc_duplicates[['huc', 'feature_id', 'point_in_wbd']])

    if not wbd_check_rows:
        huc_feature_cluster_df = huc_feature_cluster_df.sort_values(['feature_id', 'huc'])
        return huc_feature_cluster_df.drop_duplicates(subset=['feature_id'], keep='first')

    wbd_checks = pd.concat(wbd_check_rows, ignore_index=True)
    duplicate_candidates = duplicate_candidates.merge(
        wbd_checks,
        on=['huc', 'feature_id'],
        how='left',
    )
    duplicate_candidates['point_in_wbd'] = duplicate_candidates['point_in_wbd'].fillna(False)
    duplicate_candidates = duplicate_candidates.sort_values(
        ['feature_id', 'point_in_wbd', 'huc'],
        ascending=[True, False, True],
    )
    resolved_duplicates = duplicate_candidates.drop_duplicates(subset=['feature_id'], keep='first')

    non_duplicates = huc_feature_cluster_df.loc[
        ~huc_feature_cluster_df['feature_id'].isin(duplicate_feature_id_set)
    ]

    return pd.concat([non_duplicates, resolved_duplicates], ignore_index=True).drop(
        columns=['point_in_wbd'], errors='ignore'
    )


# *****************************************************************************
def _add_clusters_to_huc_features(huc_feature_df, cluster_df, pre_clip_huc8_dir, merge_how='left'):

    huc_feature_cluster_df = huc_feature_df.merge(cluster_df, on='feature_id', how=merge_how)
    huc_feature_cluster_df = _resolve_duplicate_feature_hucs(
        huc_feature_cluster_df, pre_clip_huc8_dir
    )
    huc_feature_cluster_df = huc_feature_cluster_df.rename(
        columns={'runoff_cluster_idx': 'cluster'}
    )

    return huc_feature_cluster_df[
        ['huc', 'feature_id', 'order_', 'cluster']
    ].sort_values(['huc', 'feature_id'])


# *****************************************************************************
def create_huc_feature_cluster_df(optz_metrics_dir, pre_clip_huc8_dir):

    hucs = get_optimized_hucs(optz_metrics_dir)
    huc_feature_df = _read_huc_feature_df(hucs, pre_clip_huc8_dir)

    cluster_csv_path = join(optz_metrics_dir, RECURRENCE_CLUSTER_FILENAME)
    if not os.path.exists(cluster_csv_path):
        raise FileNotFoundError(f"{RECURRENCE_CLUSTER_FILENAME} not found in {optz_metrics_dir}")

    cluster_df = _read_recurrence_flow_clusters(cluster_csv_path, huc_feature_df['feature_id'])
    huc_feature_cluster_df = _add_clusters_to_huc_features(
        huc_feature_df,
        cluster_df,
        pre_clip_huc8_dir,
    )

    all_cluster_df = _read_recurrence_flow_clusters(cluster_csv_path)
    all_huc_feature_df = _read_huc_feature_df(
        _get_pre_clip_hucs(pre_clip_huc8_dir),
        pre_clip_huc8_dir,
        all_cluster_df['feature_id'],
    )
    all_huc_feature_cluster_df = _add_clusters_to_huc_features(
        all_huc_feature_df,
        all_cluster_df,
        pre_clip_huc8_dir,
        merge_how='right',
    )

    today = datetime.date.today().strftime('%Y%m%d')
    output_csv_path = join(optz_metrics_dir, f'huc_feature_clusters_{today}.csv')
    huc_feature_cluster_df.to_csv(output_csv_path, index=False)

    all_output_csv_path = join(optz_metrics_dir, f'all_huc_feature_clusters_{today}.csv')
    all_huc_feature_cluster_df.to_csv(all_output_csv_path, index=False)

    return huc_feature_cluster_df, all_huc_feature_cluster_df


# *****************************************************************************
def create_optz_roughness_df(
    optz_metrics_dir,
    ch_n_valid_threshold=CH_N_VALID_THRESHOLD,
    ob_n_valid_threshold=OB_N_VALID_THRESHOLD,
):
    """Create one best-loss optimized roughness row per HUC."""

    pattern = join(optz_metrics_dir, "optz_iteration_metrics_*.csv")
    optz_csv_paths = sorted(glob.glob(pattern))

    if not optz_csv_paths:
        pattern = join(optz_metrics_dir, "*", "optz_iteration_metrics_*.csv")
        optz_csv_paths = sorted(glob.glob(pattern))

    if not optz_csv_paths:
        raise FileNotFoundError(
            f"No optz_iteration_metrics_*.csv files found in {optz_metrics_dir} or its child directories"
        )

    required_cols = [
        'total_loss',
        'mannN_ch_coef',
        'mannN_ob_coef',
        'mannN_ch',
        'mannN_ob',
    ]
    output_cols = [
        'huc',
        'optz_data_source',
        'optz_iteration',
        'optz_total_loss',
        'optz_mannN_ch_coef',
        'optz_mannN_ob_coef',
        'optz_mannN_ch',
        'optz_mannN_ob',
        'ch_n_valid',
        'ob_n_valid',
        'valid_iteration',
        'valid_optz_total_loss',
        'valid_optz_mannN_ch_coef',
        'valid_optz_mannN_ob_coef',
        'valid_optz_mannN_ch',
        'valid_optz_mannN_ob',
        'loss_deviation_percentage',
    ]

    ch_n_low_threshold, ch_n_up_threshold = ch_n_valid_threshold
    ob_n_low_threshold, ob_n_up_threshold = ob_n_valid_threshold

    optz_rows = []
    for optz_csv_path in optz_csv_paths: # [0:3]
        optz_res_huc = pd.read_csv(optz_csv_path, dtype={'huc': str}, low_memory=False)

        missing_cols = [col for col in required_cols if col not in optz_res_huc.columns]
        if missing_cols:
            raise ValueError(f"{optz_csv_path} is missing required columns: {missing_cols}")

        for col in required_cols:
            optz_res_huc[col] = pd.to_numeric(optz_res_huc[col], errors='raise')

        huc = os.path.basename(optz_csv_path).replace("optz_iteration_metrics_", "").replace(".csv", "")
        optz_data_source = os.path.basename(os.path.dirname(optz_csv_path)).replace("optz_final_", "")
        best_row = optz_res_huc.loc[optz_res_huc['total_loss'].idxmin()]
        ch_n_valid = ch_n_low_threshold < best_row['mannN_ch'] < ch_n_up_threshold
        ob_n_valid = ob_n_low_threshold < best_row['mannN_ob'] < ob_n_up_threshold

        if ch_n_valid and ob_n_valid:
            valid_row = best_row
        else:
            valid_optz_res_huc = optz_res_huc[
                (optz_res_huc['mannN_ch'] > ch_n_low_threshold)
                & (optz_res_huc['mannN_ch'] < ch_n_up_threshold)
                & (optz_res_huc['mannN_ob'] > ob_n_low_threshold)
                & (optz_res_huc['mannN_ob'] < ob_n_up_threshold)
            ]

            if valid_optz_res_huc.empty:
                raise ValueError(
                    f"No relaxed-valid ManningN row found in {optz_csv_path} "
                    f"using {ch_n_low_threshold} < mannN_ch < {ch_n_up_threshold} "
                    f"and {ob_n_low_threshold} < mannN_ob < {ob_n_up_threshold}"
                )

            valid_row = valid_optz_res_huc.loc[valid_optz_res_huc['total_loss'].idxmin()]

        optz_rows.append(
            {
                'huc': huc,
                'optz_data_source': optz_data_source,
                'optz_iteration': best_row['iteration'],
                'optz_total_loss': best_row['total_loss'],
                'optz_mannN_ch_coef': best_row['mannN_ch_coef'],
                'optz_mannN_ob_coef': best_row['mannN_ob_coef'],
                'optz_mannN_ch': best_row['mannN_ch'],
                'optz_mannN_ob': best_row['mannN_ob'],
                'ch_n_valid': ch_n_valid,
                'ob_n_valid': ob_n_valid,
                'valid_iteration': valid_row['iteration'],
                'valid_optz_total_loss': valid_row['total_loss'],
                'valid_optz_mannN_ch_coef': valid_row['mannN_ch_coef'],
                'valid_optz_mannN_ob_coef': valid_row['mannN_ob_coef'],
                'valid_optz_mannN_ch': valid_row['mannN_ch'],
                'valid_optz_mannN_ob': valid_row['mannN_ob'],
                'loss_deviation_percentage': 100 * (
                    valid_row['total_loss'] - best_row['total_loss']
                ) / best_row['total_loss'],
            }
        )

    optz_roughness_df = pd.DataFrame(optz_rows, columns=output_cols)
    optz_roughness_df['huc'] = optz_roughness_df['huc'].astype(str)
    rounding_cols = [
        'optz_mannN_ch_coef',
        'optz_mannN_ob_coef',
        'optz_mannN_ch',
        'optz_mannN_ob',
        'valid_optz_mannN_ch_coef',
        'valid_optz_mannN_ob_coef',
        'valid_optz_mannN_ch',
        'valid_optz_mannN_ob',
    ]
    optz_roughness_df[rounding_cols] = optz_roughness_df[rounding_cols].round(4)
    optz_roughness_df['loss_deviation_percentage'] = optz_roughness_df[
        'loss_deviation_percentage'
    ].round(2)

    today = datetime.date.today().strftime('%Y%m%d')
    output_csv_path = join(optz_metrics_dir, f'optz_roughness_summary_{today}.csv')
    optz_roughness_df.to_csv(output_csv_path, index=False)

    return optz_roughness_df


# *****************************************************************************
def add_optz_roughness_to_huc_features(
    huc_feature_cluster_df,
    optz_roughness_df,
    loss_deviation_threshold=LOSS_DEVIATION_THRESHOLD,
    optz_metrics_dir=OPTZ_METRICS_DIR,
):
    # Reads huc_feature_cluster_df for hucs that have optz roughness
    # huc_feature_clusters_csv_path = join(optz_metrics_dir, 'huc_feature_clusters_20260626.csv')
    # huc_feature_cluster_df = pd.read_csv(huc_feature_clusters_csv_path)
    required_huc_feature_cols = ['huc', 'feature_id']
    required_optz_cols = [
        'huc',
        'optz_data_source',
        'loss_deviation_percentage',
        'optz_mannN_ch',
        'optz_mannN_ob',
        'valid_optz_mannN_ch',
        'valid_optz_mannN_ob',
    ]

    missing_huc_feature_cols = [
        col for col in required_huc_feature_cols if col not in huc_feature_cluster_df.columns
    ]
    if missing_huc_feature_cols:
        raise ValueError(
            f"huc_feature_cluster_df is missing required columns: {missing_huc_feature_cols}"
        )

    missing_optz_cols = [col for col in required_optz_cols if col not in optz_roughness_df.columns]
    if missing_optz_cols:
        raise ValueError(f"optz_roughness_df is missing required columns: {missing_optz_cols}")

    huc_feature_cluster_df = huc_feature_cluster_df.copy()
    optz_roughness_df = optz_roughness_df.copy()
    huc_feature_cluster_df['huc'] = huc_feature_cluster_df['huc'].astype(str)
    optz_roughness_df['huc'] = optz_roughness_df['huc'].astype(str)

    optz_roughness_df['loss_deviation_percentage'] = pd.to_numeric(
        optz_roughness_df['loss_deviation_percentage'], errors='raise'
    )

    use_valid_optz = optz_roughness_df['loss_deviation_percentage'] < loss_deviation_threshold
    optz_roughness_df['selected_optz_mannN_ch'] = np.where(
        use_valid_optz,
        optz_roughness_df['valid_optz_mannN_ch'],
        optz_roughness_df['optz_mannN_ch'],
    )
    optz_roughness_df['selected_optz_mannN_ob'] = np.where(
        use_valid_optz,
        optz_roughness_df['valid_optz_mannN_ob'],
        optz_roughness_df['optz_mannN_ob'],
    )

    duplicated_sources = optz_roughness_df.duplicated(['huc', 'optz_data_source'], keep=False)
    if duplicated_sources.any():
        duplicate_keys = optz_roughness_df.loc[
            duplicated_sources, ['huc', 'optz_data_source']
        ].drop_duplicates()
        raise ValueError(
            "optz_roughness_df has duplicate huc/source rows: "
            f"{duplicate_keys.head(10).to_dict('records')}"
        )

    optz_wide_df = optz_roughness_df.pivot(
        index='huc',
        columns='optz_data_source',
        values=['selected_optz_mannN_ch', 'selected_optz_mannN_ob'],
    )
    optz_wide_df.columns = [
        f"optz_mannN_ch_{source}" if value_col == 'selected_optz_mannN_ch'
        else f"optz_mannN_ob_{source}"
        for value_col, source in optz_wide_df.columns
    ]
    optz_wide_df = optz_wide_df.reset_index()

    expected_optz_cols = [
        'optz_mannN_ch_ble',
        'optz_mannN_ch_ahps',
        'optz_mannN_ob_ble',
        'optz_mannN_ob_ahps',
    ]
    for col in expected_optz_cols:
        if col not in optz_wide_df.columns:
            optz_wide_df[col] = np.nan

    optz_wide_df = optz_wide_df[['huc'] + expected_optz_cols]
    huc_feature_optz_df = huc_feature_cluster_df.merge(
        optz_wide_df,
        on='huc',
        how='left',
    )

    huc_feature_optz_df[expected_optz_cols] = huc_feature_optz_df[expected_optz_cols].round(4)

    today = datetime.date.today().strftime('%Y%m%d')
    output_csv_path = join(optz_metrics_dir, f'huc_feature_clusters_with_optz_roughness_{today}.csv')
    huc_feature_optz_df.to_csv(output_csv_path, index=False)

    return huc_feature_optz_df


# *****************************************************************************
def _mean_col(df, col):
    return df[col].dropna().mean()

def _mean_cols(df, cols):
    values = pd.concat([df[col] for col in cols], ignore_index=True).dropna()
    if values.empty:
        return np.nan
    return values.mean()


# *****************************************************************************
def create_cluster_order_optz_roughness_df(huc_feature_optz_df, optz_metrics_dir):
    required_cols = [
        'feature_id',
        'cluster',
        'order_',
        'optz_mannN_ch_ble',
        'optz_mannN_ch_ahps',
        'optz_mannN_ob_ble',
        'optz_mannN_ob_ahps',
    ]
    output_cols = [
        'cluster',
        'optz_mannN_ch_>3',
        'optz_mannN_ch_<=3',
        'optz_mannN_ob_>3',
        'optz_mannN_ob_<=3',
    ]

    missing_cols = [col for col in required_cols if col not in huc_feature_optz_df.columns]
    if missing_cols:
        raise ValueError(f"huc_feature_optz_df is missing required columns: {missing_cols}")

    huc_feature_optz_df = huc_feature_optz_df.copy()
    huc_feature_optz_df = huc_feature_optz_df.dropna(subset=['cluster'])
    huc_feature_optz_df = huc_feature_optz_df.drop_duplicates(subset=['feature_id'])
    huc_feature_optz_df['cluster'] = pd.to_numeric(
        huc_feature_optz_df['cluster'], errors='raise'
    ).astype('int64')
    huc_feature_optz_df['order_'] = pd.to_numeric(
        huc_feature_optz_df['order_'], errors='raise'
    ).astype('int64')

    roughness_cols = [
        'optz_mannN_ch_ble',
        'optz_mannN_ch_ahps',
        'optz_mannN_ob_ble',
        'optz_mannN_ob_ahps',
    ]
    for col in roughness_cols:
        huc_feature_optz_df[col] = pd.to_numeric(huc_feature_optz_df[col], errors='coerce')

    # ahps clusters: -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    # ahps clusters, not ble: -1, 0, 1, 2, 8, 10
    # ahps and ble clusters: 3, 4, 5, 6, 7, 9, 11
    cluster_rows = []
    for cluster, cluster_df in huc_feature_optz_df.groupby('cluster'):

        has_ble = cluster_df[['optz_mannN_ch_ble', 'optz_mannN_ob_ble']].notna().any().any()
        has_ahps = cluster_df[['optz_mannN_ch_ahps', 'optz_mannN_ob_ahps']].notna().any().any()

        if has_ble and not has_ahps:
            optz_mannN_ch = _mean_col(cluster_df, 'optz_mannN_ch_ble')
            optz_mannN_ob = _mean_col(cluster_df, 'optz_mannN_ob_ble')
            cluster_row = {
                'cluster': cluster,
                'optz_mannN_ch_>3': optz_mannN_ch,
                'optz_mannN_ch_<=3': optz_mannN_ch,
                'optz_mannN_ob_>3': optz_mannN_ob,
                'optz_mannN_ob_<=3': optz_mannN_ob,
            }
            # print(cluster_row)
        elif has_ahps and not has_ble:
            optz_mannN_ch = _mean_col(cluster_df, 'optz_mannN_ch_ahps')
            optz_mannN_ob = _mean_col(cluster_df, 'optz_mannN_ob_ahps')
            cluster_row = {
                'cluster': cluster,
                'optz_mannN_ch_>3': optz_mannN_ch,
                'optz_mannN_ch_<=3': optz_mannN_ch,
                'optz_mannN_ob_>3': optz_mannN_ob,
                'optz_mannN_ob_<=3': optz_mannN_ob,
            }
            # print(cluster_row)
        elif has_ble and has_ahps:
            order_lte_3_df = cluster_df.loc[cluster_df['order_'] <= 3]
            order_gt_3_df = cluster_df.loc[cluster_df['order_'] > 3]
            cluster_row = {
                'cluster': cluster,
                'optz_mannN_ch_>3': _mean_cols(
                    order_gt_3_df,
                    ['optz_mannN_ch_ble', 'optz_mannN_ch_ahps'],
                ),
                'optz_mannN_ch_<=3': _mean_col(order_lte_3_df, 'optz_mannN_ch_ble'),
                'optz_mannN_ob_>3': _mean_cols(
                    order_gt_3_df,
                    ['optz_mannN_ob_ble', 'optz_mannN_ob_ahps'],
                ),
                'optz_mannN_ob_<=3': _mean_col(order_lte_3_df, 'optz_mannN_ob_ble'),
            }
        else:
            cluster_row = {
                'cluster': cluster,
                'optz_mannN_ch_>3': np.nan,
                'optz_mannN_ch_<=3': np.nan,
                'optz_mannN_ob_>3': np.nan,
                'optz_mannN_ob_<=3': np.nan,
            }

        cluster_rows.append(cluster_row)

    cluster_order_optz_roughness_df = pd.DataFrame(cluster_rows, columns=output_cols)
    rounding_cols = [
        'optz_mannN_ch_>3',
        'optz_mannN_ch_<=3',
        'optz_mannN_ob_>3',
        'optz_mannN_ob_<=3',
    ]
    cluster_order_optz_roughness_df[rounding_cols] = cluster_order_optz_roughness_df[
        rounding_cols
    ].round(4)

    today = datetime.date.today().strftime('%Y%m%d')
    output_csv_path = join(optz_metrics_dir, f'cluster_order_optz_roughness_{today}.csv')
    cluster_order_optz_roughness_df.to_csv(output_csv_path, index=False)

    return cluster_order_optz_roughness_df


# *****************************************************************************
def update_mannings_with_cluster_order_optz_roughness(
    all_huc_feature_cluster_df,
    cluster_order_optz_roughness_df,
    optz_mannings_path=join(OPTZ_METRICS_DIR, OPTZ_MANNINGS_FILENAME),
    output_csv_path=None,
    ohio_allegeny_monongahela_mannings_path=join(
        OPTZ_METRICS_DIR, OHIO_ALLEGENY_MONONGAHELA_RIVERS_FILENAME
    ),
    channel_n_ohio_allegeny_monongahela=CHANNEL_N_OHIO_ALLEGENY_MONONGAHELA,
    overbank_n_ohio_allegeny_monongahela=OVERBANK_N_OHIO_ALLEGENY_MONONGAHELA,
):

    required_mannings_cols = ['feature_id', 'channel_n', 'overbank_n']
    required_feature_cols = ['feature_id', 'cluster', 'order_']
    required_cluster_cols = [
        'cluster',
        'optz_mannN_ch_>3',
        'optz_mannN_ch_<=3',
        'optz_mannN_ob_>3',
        'optz_mannN_ob_<=3',
    ]

    if not os.path.exists(optz_mannings_path):
        raise FileNotFoundError(f"{optz_mannings_path} not found")

    mannings_df = pd.read_csv(optz_mannings_path)
    missing_mannings_cols = [col for col in required_mannings_cols if col not in mannings_df.columns]
    if missing_mannings_cols:
        raise ValueError(f"{optz_mannings_path} is missing required columns: {missing_mannings_cols}")

    # all_huc_feature_cluster_path = join(optz_metrics_dir, 'all_huc_feature_clusters_20260626.csv')
    # all_huc_feature_cluster_df = pd.read_csv(all_huc_feature_cluster_path)
    missing_feature_cols = [
        col for col in required_feature_cols if col not in all_huc_feature_cluster_df.columns
    ]
    if missing_feature_cols:
        raise ValueError(
            f"all_huc_feature_cluster_df is missing required columns: {missing_feature_cols}"
        )

    missing_cluster_cols = [
        col for col in required_cluster_cols if col not in cluster_order_optz_roughness_df.columns
    ]
    if missing_cluster_cols:
        raise ValueError(
            "cluster_order_optz_roughness_df is missing required columns: "
            f"{missing_cluster_cols}"
        )

    mannings_df = mannings_df.copy()
    mannings_df['feature_id'] = pd.to_numeric(mannings_df['feature_id'], errors='raise').astype('int64')
    mannings_df['channel_n'] = pd.to_numeric(mannings_df['channel_n'], errors='raise')
    mannings_df['overbank_n'] = pd.to_numeric(mannings_df['overbank_n'], errors='raise')

    feature_lookup_df = all_huc_feature_cluster_df[required_feature_cols].copy()
    feature_lookup_df['feature_id'] = pd.to_numeric(
        feature_lookup_df['feature_id'], errors='raise'
    ).astype('int64')
    feature_lookup_df['cluster'] = pd.to_numeric(feature_lookup_df['cluster'], errors='coerce')
    feature_lookup_df['order_'] = pd.to_numeric(feature_lookup_df['order_'], errors='coerce')
    feature_lookup_df = feature_lookup_df.dropna(subset=['cluster', 'order_']).copy()
    feature_lookup_df['cluster'] = feature_lookup_df['cluster'].astype('int64')
    feature_lookup_df['order_'] = feature_lookup_df['order_'].astype('int64')

    duplicated_features = feature_lookup_df.duplicated('feature_id', keep=False)
    if duplicated_features.any():
        duplicate_feature_ids = feature_lookup_df.loc[
            duplicated_features, 'feature_id'
        ].drop_duplicates()
        raise ValueError(
            "all_huc_feature_cluster_df has duplicate feature_id rows: "
            f"{duplicate_feature_ids.head(10).tolist()}"
        )

    cluster_lookup_df = cluster_order_optz_roughness_df[required_cluster_cols].copy()
    cluster_lookup_df['cluster'] = pd.to_numeric(
        cluster_lookup_df['cluster'], errors='raise'
    ).astype('int64')
    for col in required_cluster_cols[1:]:
        cluster_lookup_df[col] = pd.to_numeric(cluster_lookup_df[col], errors='coerce')

    duplicated_clusters = cluster_lookup_df.duplicated('cluster', keep=False)
    if duplicated_clusters.any():
        duplicate_clusters = cluster_lookup_df.loc[
            duplicated_clusters, 'cluster'
        ].drop_duplicates()
        raise ValueError(
            "cluster_order_optz_roughness_df has duplicate cluster rows: "
            f"{duplicate_clusters.head(10).tolist()}"
        )
    
    cluster_lookup_df[cluster_lookup_df.columns[1:]] = cluster_lookup_df[cluster_lookup_df.columns[1:]].round(3)
    cluster_lookup_df.loc[cluster_lookup_df["cluster"].between(1, 4), ["optz_mannN_ch_>3", "optz_mannN_ch_<=3", "optz_mannN_ob_>3", "optz_mannN_ob_<=3"]] = [0.033, 0.036, 0.103, 0.095]

    feature_roughness_df = feature_lookup_df.merge(cluster_lookup_df, on='cluster', how='left')
    order_gt_3 = feature_roughness_df['order_'] > 3
    feature_roughness_df['updated_channel_n'] = np.where(
        order_gt_3,
        feature_roughness_df['optz_mannN_ch_>3'],
        feature_roughness_df['optz_mannN_ch_<=3'],
    )
    feature_roughness_df['updated_overbank_n'] = np.where(
        order_gt_3,
        feature_roughness_df['optz_mannN_ob_>3'],
        feature_roughness_df['optz_mannN_ob_<=3'],
    )
    feature_roughness_df = feature_roughness_df[
        ['feature_id', 'updated_channel_n', 'updated_overbank_n']
    ]

    updated_mannings_df = mannings_df.merge(feature_roughness_df, on='feature_id', how='left')
    channel_update_mask = updated_mannings_df['updated_channel_n'].notna()
    overbank_update_mask = updated_mannings_df['updated_overbank_n'].notna()
    updated_mannings_df.loc[channel_update_mask, 'channel_n'] = updated_mannings_df.loc[
        channel_update_mask, 'updated_channel_n'
    ]
    updated_mannings_df.loc[overbank_update_mask, 'overbank_n'] = updated_mannings_df.loc[
        overbank_update_mask, 'updated_overbank_n'
    ]
    updated_mannings_df = updated_mannings_df.drop(
        columns=['updated_channel_n', 'updated_overbank_n']
    )

    if not os.path.exists(ohio_allegeny_monongahela_mannings_path):
        raise FileNotFoundError(f"{ohio_allegeny_monongahela_mannings_path} not found")

    ohio_allegeny_monongahela_df = pd.read_csv(
        ohio_allegeny_monongahela_mannings_path,
        usecols=['feature_id'],
    )
    ohio_allegeny_monongahela_df['feature_id'] = pd.to_numeric(
        ohio_allegeny_monongahela_df['feature_id'], errors='raise'
    ).astype('int64')
    ohio_allegeny_monongahela_feature_ids = set(
        ohio_allegeny_monongahela_df['feature_id'].drop_duplicates()
    )
    ohio_allegeny_monongahela_update_mask = updated_mannings_df['feature_id'].isin(
        ohio_allegeny_monongahela_feature_ids
    )
    updated_mannings_df.loc[ohio_allegeny_monongahela_update_mask, 'channel_n'] = (
        channel_n_ohio_allegeny_monongahela
    )
    updated_mannings_df.loc[ohio_allegeny_monongahela_update_mask, 'overbank_n'] = (
        overbank_n_ohio_allegeny_monongahela
    )

    updated_mannings_df['channel_n'] = updated_mannings_df['channel_n'].fillna(CHANNEL_N_NAN_FILL)
    updated_mannings_df['overbank_n'] = updated_mannings_df['overbank_n'].fillna(OVERBANK_N_NAN_FILL)

    updated_mannings_df[['channel_n', 'overbank_n']] = updated_mannings_df[
        ['channel_n', 'overbank_n']
    ].round(3)

    if output_csv_path is None:
        output_csv_path = join(
            os.path.dirname(optz_mannings_path),
            OPTZ_MANNINGS_OUTPUT_FILENAME,
        )

    updated_mannings_df[required_mannings_cols].to_csv(output_csv_path, index=False)


# *****************************************************************************
def analyze_optz_roughness(
    optz_metrics_dir=OPTZ_METRICS_DIR,
    pre_clip_huc8_dir=PRE_CLIP_HUC8_DIR,
):

    huc_feature_cluster_df, all_huc_feature_cluster_df = create_huc_feature_cluster_df(
        optz_metrics_dir=optz_metrics_dir,
        pre_clip_huc8_dir=pre_clip_huc8_dir,
    )
    optz_roughness_df = create_optz_roughness_df(optz_metrics_dir=optz_metrics_dir)
    huc_feature_optz_df = add_optz_roughness_to_huc_features(
        huc_feature_cluster_df,
        optz_roughness_df,
        optz_metrics_dir=optz_metrics_dir,
    )
    cluster_order_optz_roughness_df = create_cluster_order_optz_roughness_df(
        huc_feature_optz_df,
        optz_metrics_dir=optz_metrics_dir,
    )
    update_mannings_with_cluster_order_optz_roughness(
        all_huc_feature_cluster_df,
        cluster_order_optz_roughness_df,
        optz_mannings_path=join(optz_metrics_dir, OPTZ_MANNINGS_FILENAME),
        output_csv_path=join(optz_metrics_dir, OPTZ_MANNINGS_OUTPUT_FILENAME),
        ohio_allegeny_monongahela_mannings_path=join(
            optz_metrics_dir, OHIO_ALLEGENY_MONONGAHELA_RIVERS_FILENAME
        ),
    )


# *****************************************************************************
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Analyze optimized manning roughness for in-channel and overbank"
    )
    parser.add_argument('-metrics_dir', '--metrics_dir', help='Path to a directory containing optimized metrics and roughness', required=True, type=str)
    parser.add_argument('-preclip_huc8_dir', '--preclip_huc8_dir', help='Path to preclip_huc8_dir', required=True, type=str)

    args = vars(parser.parse_args())
    analysis_dir = args['metrics_dir']
    preclip_huc8_dir = args['preclip_huc8_dir']


    analyze_optz_roughness(analysis_dir, preclip_huc8_dir)
    