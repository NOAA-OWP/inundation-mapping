import argparse
import os
import re

import pandas as pd


def process_branch(sub_branch_path, branch, huc_id):
    src_base_file = os.path.join(sub_branch_path, f'src_base_{branch}.csv')
    hydro_table_file = os.path.join(sub_branch_path, f'hydroTable_{branch}.csv')
    src_full_file = os.path.join(sub_branch_path, f'src_full_crosswalked_{branch}.csv')
    small_segments_file = os.path.join(sub_branch_path, f'small_segments_{branch}.csv')

    # A branch may have failed and these files may not exist. It might be a known captured
    # branch error such as FIM codes 61, 62, etc.
    # or may be a legit new bug.
    # If any of these files are missing, skip trying to update it.
    # Don't really need to log it as the original fail is alreayd logged earlier.
    if not all(os.path.exists(f) for f in [src_base_file, src_full_file, hydro_table_file]):
        return

    # Load input files
    input_src_base = pd.read_csv(src_base_file, dtype={'CatchId': str})
    input_src_full = pd.read_csv(src_full_file)
    input_hydro_table = pd.read_csv(hydro_table_file, dtype={'HydroID': str})

    # Clean column headers of any leading/trailing whitespace.
    input_src_base.columns = input_src_base.columns.str.strip()
    input_src_full.columns = input_src_full.columns.str.strip()
    input_hydro_table.columns = input_hydro_table.columns.str.strip()

    # Apply data types to the clean column names
    id_cols = ['HydroID', 'NextDownID', 'feature_id']
    for col in id_cols:
        if col in input_src_full.columns:
            input_src_full[col] = input_src_full[col].astype(str)

    # make sure numeric columns of src_base are really numeric
    input_src_base = input_src_base.rename(columns=lambda x: x.strip(" "))
    numeric_cols = [col for col in input_src_base.columns if col not in ['CatchId', 'HydroID', 'NextDownID']]
    input_src_base[numeric_cols] = input_src_base[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # get all fields of src_base (excluding SLOPE) and the rest from src_full
    # remove slope of src base and rename cathid to match hydroid of src_full for merge
    input_src_base = input_src_base.drop(columns=['SLOPE'])
    input_src_base.rename(columns={'CatchId': 'HydroID'}, inplace=True)
    recalc_df = input_src_base.copy()

    recalc_df = recalc_df.merge(
        input_src_full[['HydroID', 'Stage', 'default_SLOPE', 'default_ManningN', 'NextDownID', 'order_']],
        on=['HydroID', 'Stage'],
    )

    # If the merge failed, Exit gracefully.
    if recalc_df.empty:
        print(
            f"Warning: Merge failed for branch {branch} for huc_id {huc_id}."
            " No matching CatchId/HydroID found. Skipping."
        )
        return

    recalc_df.rename(columns={'default_SLOPE': 'SLOPE', 'default_ManningN': 'ManningN'}, inplace=True)

    # now continue with calculations
    recalc_df['TopWidth (m)'] = recalc_df['SurfaceArea (m2)'] / recalc_df['LENGTHKM'] / 1000
    recalc_df['WettedPerimeter (m)'] = recalc_df['BedArea (m2)'] / recalc_df['LENGTHKM'] / 1000
    recalc_df['WetArea (m2)'] = recalc_df['Volume (m3)'] / recalc_df['LENGTHKM'] / 1000
    recalc_df['HydraulicRadius (m)'] = recalc_df['WetArea (m2)'] / recalc_df['WettedPerimeter (m)']
    recalc_df['HydraulicRadius (m)'] = recalc_df['HydraulicRadius (m)'].fillna(0)

    recalc_df['Discharge (m3s-1)'] = (
        recalc_df['WetArea (m2)']
        * pow(recalc_df['HydraulicRadius (m)'], 2.0 / 3)
        * pow(recalc_df['SLOPE'], 0.5)
        / recalc_df['ManningN']
    )
    recalc_df.loc[recalc_df['Stage'] == 0, 'Discharge (m3s-1)'] = 0

    # Apply Small Segment Adjustments
    if os.path.exists(small_segments_file):
        sml_segs = pd.read_csv(small_segments_file, dtype=str)
        if not sml_segs.empty:
            if huc_id.startswith('19'):  # Alaska
                print("Update rating curves for short reaches in Alaska.")
                # Create a DataFrame with new values for discharge based on 'update_id'
                new_values = recalc_df[recalc_df['HydroID'].isin(sml_segs['update_id'])][
                    ['HydroID', 'Stage', 'Discharge (m3s-1)']
                ]
                # Merge this new values DataFrame with sml_segs on 'update_id' and 'HydroID'
                sml_segs_with_values = sml_segs.merge(
                    new_values, left_on='update_id', right_on='HydroID', suffixes=('', '_new')
                )
                sml_segs_with_values = sml_segs_with_values[['short_id', 'Stage', 'Discharge (m3s-1)']]
                merged_recalc_df = recalc_df.merge(
                    sml_segs_with_values[['short_id', 'Stage', 'Discharge (m3s-1)']],
                    left_on=['HydroID', 'Stage'],
                    right_on=['short_id', 'Stage'],
                    suffixes=('', '_df2'),
                )
                merged_recalc_df = merged_recalc_df[['HydroID', 'Stage', 'Discharge (m3s-1)_df2']]
                recalc_df = pd.merge(recalc_df, merged_recalc_df, on=['HydroID', 'Stage'], how='left')

                del merged_recalc_df

                recalc_df['Discharge (m3s-1)'] = recalc_df['Discharge (m3s-1)_df2'].fillna(
                    recalc_df['Discharge (m3s-1)']
                )
                recalc_df = recalc_df.drop(columns=['Discharge (m3s-1)_df2'])
            else:
                for index, segment in sml_segs.iterrows():  # Conus
                    short_id = segment.iloc[0]
                    update_id = segment.iloc[1]
                    new_values = recalc_df.loc[recalc_df['HydroID'] == update_id][
                        ['Stage', 'Discharge (m3s-1)']
                    ]

                    for src_index, src_stage in new_values.iterrows():
                        recalc_df.loc[
                            (recalc_df['HydroID'] == short_id) & (recalc_df['Stage'] == src_stage.iloc[0]),
                            ['Discharge (m3s-1)'],
                        ] = src_stage.iloc[1]

    # Update src_full & hydroTable
    input_src_full.set_index(['HydroID', 'Stage'], inplace=True)
    recalc_df.set_index(['HydroID', 'Stage'], inplace=True)

    # Update src_full with ALL newly calculated values, aligning on the index
    input_src_full.update(recalc_df)
    input_src_full.reset_index(inplace=True)
    input_src_full['Bathymetry_source'] = pd.NA

    # Merge the final discharge values into the hydro table
    input_hydro_table = input_hydro_table.merge(
        recalc_df[['Discharge (m3s-1)']],
        left_on=['HydroID', 'stage'],
        right_on=['HydroID', 'Stage'],
        how='left',
    )

    input_hydro_table['discharge_cms'] = input_hydro_table['Discharge (m3s-1)']
    input_hydro_table['default_discharge_cms'] = input_hydro_table['Discharge (m3s-1)']
    input_hydro_table['subdiv_discharge_cms'] = pd.NA

    # Reset calibration columns to default states
    if 'precalb_discharge_cms' in input_hydro_table.columns:
        input_hydro_table['precalb_discharge_cms'] = pd.NA
    if 'calb_coef_usgs' in input_hydro_table.columns:
        input_hydro_table['calb_coef_usgs'] = pd.NA
    if 'calb_coef_spatial' in input_hydro_table.columns:
        input_hydro_table['calb_coef_spatial'] = pd.NA
    if 'calb_coef_ras2fim' in input_hydro_table.columns:
        input_hydro_table['calb_coef_ras2fim'] = pd.NA
    if 'calb_coef_final' in input_hydro_table.columns:
        input_hydro_table['calb_coef_final'] = pd.NA
    if 'calb_applied' in input_hydro_table.columns:
        input_hydro_table['calb_applied'] = False
    if 'last_updated' in input_hydro_table.columns:
        input_hydro_table['last_updated'] = pd.NA
    if 'submitter' in input_hydro_table.columns:
        input_hydro_table['submitter'] = pd.NA
    if 'obs_source' in input_hydro_table.columns:
        input_hydro_table['obs_source'] = pd.NA

    input_hydro_table.drop(columns=['Discharge (m3s-1)'], inplace=True)

    # Save updated files
    src_full_preserve_columns = [
        'SLOPE_RISE_RUN',
        'ManningN',
        'HydroID',
        'NextDownID',
        'order_',
        'SLOPE_HFAB',
        'SLOPE_IRIS_SWORD',
        'SLOPE',
        'feature_id',
        'Stage',
        'Number of Cells',
        'SurfaceArea (m2)',
        'LENGTHKM',
        'AREASQKM',
        'Volume (m3)',
        'BedArea (m2)',
        'TopWidth (m)',
        'WettedPerimeter (m)',
        'WetArea (m2)',
        'HydraulicRadius (m)',
        'Discharge (m3s-1)',
        'Bathymetry_source',
        'default_SLOPE',
        'default_ManningN',
    ]
    final_src_full = input_src_full[src_full_preserve_columns]
    final_src_full.to_csv(src_full_file, index=False)
    input_hydro_table.to_csv(hydro_table_file, index=False)


def reset_hydro_and_src(huc_path):
    print(huc_path)
    huc_id = os.path.basename(os.path.normpath(huc_path))
    branches_path = os.path.join(huc_path, 'branches')
    branch_nos = [
        branch_no
        for branch_no in os.listdir(branches_path)
        if os.path.isdir(os.path.join(huc_path, 'branches', branch_no))
    ]
    for branch_no in branch_nos:
        sub_branch_path = os.path.join(branches_path, branch_no)
        process_branch(sub_branch_path, branch_no, huc_id)


if __name__ == '__main__':
    '''
    Sample usage (min params):
        python3 src/update_htable_src.py
            -huc_dir /data/previous_fim/fim_4_5_2_0
    '''
    parser = argparse.ArgumentParser(description='Update hydrotable and src files.')
    parser.add_argument(
        '-huc_dir', '--huc_dir', help='Directory path for fim output for a HUC.', required=True
    )
    args = parser.parse_args()
    reset_hydro_and_src(args.huc_dir)
