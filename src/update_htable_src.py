import argparse
import os
import re

import geopandas as gpd
import pandas as pd


def process_branch(sub_branch_path, branch):
    src_base_file = os.path.join(sub_branch_path, f'src_base_{branch}.csv')
    hydro_table_file = os.path.join(sub_branch_path, f'hydroTable_{branch}.csv')
    src_full_file = os.path.join(sub_branch_path, f'src_full_crosswalked_{branch}.csv')
    input_flows_file = os.path.join(
        sub_branch_path, f'demDerived_reaches_split_filtered_addedAttributes_crosswalked_{branch}.gpkg'
    )
    # print(str(branch))

    src_full_preserve_columns = [
        'Stage',
        # 'Number of Cells',
        # 'SurfaceArea (m2)',
        # 'BedArea (m2)',
        # 'Volume (m3)',
        'SLOPE_RISE_RUN',
        # 'LENGTHKM',
        # 'AREASQKM',
        'ManningN',
        'HydroID',
        'NextDownID',
        'order_',
        'SLOPE_HFAB',
        'SLOPE_IRIS_SWORD',
        'SLOPE',
        'TopWidth (m)',
        'WettedPerimeter (m)',
        'WetArea (m2)',
        'HydraulicRadius (m)',
        'Discharge (m3s-1)',
        'Bathymetry_source',
        'feature_id',
    ]

    # A branch may have failed and these files may not exist. It might be a known captured
    # branch error such as FIM codes 61, 62, etc.
    # or may be a legit new bug.
    # If any of these files are missing, skip trying to update it.
    # Don't really need to log it as the original fail is alreayd logged earlier.

    if (
        (os.path.exists(src_base_file) is False)
        or (os.path.exists(src_full_file) is False)
        or (os.path.exists(hydro_table_file) is False)
        or (os.path.exists(input_flows_file) is False)
    ):
        return

    input_src_base = pd.read_csv(src_base_file, dtype=object)
    # Check available columns
    with open(src_full_file, 'r') as f:
        first_line = f.readline().strip()
        actual_columns = first_line.split(',')
    missing_columns = [col for col in src_full_preserve_columns if col not in actual_columns]
    if missing_columns:
        print(
            f"Warning: The following columns are missing from the file and will be skipped: {missing_columns}"
        )

    # Filter only available columns
    available_columns = [col for col in src_full_preserve_columns if col in actual_columns]

    input_src_full = pd.read_csv(src_full_file, dtype=object, usecols=available_columns)
    input_hydro_table = pd.read_csv(hydro_table_file, dtype=object)
    input_flows = gpd.read_file(input_flows_file, engine="pyogrio", use_arrow=True)

    input_src_base = input_src_base.merge(
        input_flows[['ManningN', 'HydroID', 'NextDownID', 'order_']], left_on='CatchId', right_on='HydroID'
    )

    # Update src_full
    input_src_base = input_src_base.rename(columns=lambda x: x.strip(" "))
    input_src_base = input_src_base.apply(pd.to_numeric, **{'errors': 'coerce'})
    input_src_full['SLOPE'] = input_src_full['SLOPE'].astype(float)
    input_src_full['Number of Cells'] = input_src_base['Number of Cells']
    input_src_full['SurfaceArea (m2)'] = input_src_base['SurfaceArea (m2)']
    input_src_full['LENGTHKM'] = input_src_base['LENGTHKM']
    input_src_full['AREASQKM'] = input_src_base['AREASQKM']
    input_src_full['Volume (m3)'] = input_src_base['Volume (m3)']
    input_src_full['BedArea (m2)'] = input_src_base['BedArea (m2)']
    input_src_full['TopWidth (m)'] = input_src_base['SurfaceArea (m2)'] / input_src_base['LENGTHKM'] / 1000
    input_src_full['WettedPerimeter (m)'] = input_src_base['BedArea (m2)'] / input_src_base['LENGTHKM'] / 1000
    input_src_full['WetArea (m2)'] = input_src_base['Volume (m3)'] / input_src_base['LENGTHKM'] / 1000
    input_src_full['HydraulicRadius (m)'] = (
        input_src_full['WetArea (m2)'] / input_src_full['WettedPerimeter (m)']
    )
    input_src_full['HydraulicRadius (m)'].fillna(0, inplace=True)
    input_src_full['Discharge (m3s-1)'] = (
        input_src_full['WetArea (m2)']
        * pow(input_src_full['HydraulicRadius (m)'], 2.0 / 3)
        * pow(input_src_full['SLOPE'], 0.5)
        / input_src_base['ManningN']
    )
    input_src_full['Bathymetry_source'] = pd.NA
    # input_src_full = input_src_full.iloc[:, :19]

    # Update hydroTable
    input_hydro_table['subdiv_discharge_cms'] = pd.NA
    input_hydro_table['discharge_cms'] = input_hydro_table['default_discharge_cms']

    # Save updated files
    input_src_full.to_csv(src_full_file, index=False)
    input_hydro_table.to_csv(hydro_table_file, index=False)


# TODO: May 16, 2025: add mp and glob to speed this way up
def reset_hydro_and_src(huc_path):
    # hucs = [h for h in os.listdir(fim_dir) if re.match(r'^\d{8}$', h)]
    # for huc_folder in hucs:
    #     huc_path = os.path.join(fim_dir, huc_folder)
    #     if os.path.isdir(huc_path):
    #         for branch_folder in os.listdir(huc_path):
    #             branch_path = os.path.join(huc_path, branch_folder)
    #             if os.path.isdir(branch_path):
    #                 for branch in os.listdir(branch_path):
    #                     sub_branch_path = os.path.join(branch_path, branch)
    #                     if os.path.isdir(sub_branch_path):
    #                         process_branch(sub_branch_path, branch)        

    branches_path = os.path.join(huc_path, 'branches')
    branch_nos=[branch_no for branch_no in os.listdir(branches_path) if os.path.isdir(os.path.join(huc_path,'branches', branch_no))]
    for branch_no in branch_nos:
        sub_branch_path=os.path.join(branches_path, branch_no)
        process_branch(sub_branch_path, branch_no)


# Example usage:
# reset_hydro_and_src('/path/to/fim_dir')
if __name__ == '__main__':
    '''
    Sample usage (min params):
        python3 src/update_htable_src.py
            -huc_dir /data/previous_fim/fim_4_5_2_0
    '''

    # TODO: May 16, 2025
    # Add MP, try/except and logging to file only here
    # We can't do prints really as it doesn't get back to bash correctly.
    # Make sure log file name has a datetime stamp it in, in case it is run a second time.

    parser = argparse.ArgumentParser(description='Update hydrotable and src files.')
    parser.add_argument('-huc_dir', '--huc_dir', help='Directory path for fim output for a HUC.', required=True)

    args = parser.parse_args()

    reset_hydro_and_src(args.huc_dir)
