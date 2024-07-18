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

    input_src_base = pd.read_csv(src_base_file, dtype=object)
    input_src_full = pd.read_csv(src_full_file, dtype=object)
    input_hydro_table = pd.read_csv(hydro_table_file, dtype=object)
    input_flows = gpd.read_file(input_flows_file, engine="pyogrio", use_arrow=True)

    input_src_base = input_src_base.merge(
        input_flows[['ManningN', 'HydroID', 'NextDownID', 'order_']], left_on='CatchId', right_on='HydroID'
    )

    # Update src_full
    input_src_base = input_src_base.rename(columns=lambda x: x.strip(" "))
    input_src_base = input_src_base.apply(pd.to_numeric, **{'errors': 'coerce'})
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
        * pow(input_src_base['SLOPE'], 0.5)
        / input_src_base['ManningN']
    )
    input_src_full['Bathymetry_source'] = pd.NA
    input_src_full = input_src_full.iloc[:, :19]

    # Update hydroTable
    input_hydro_table['subdiv_discharge_cms'] = pd.NA
    input_hydro_table['discharge_cms'] = input_hydro_table['default_discharge_cms']

    # Save updated files
    input_src_full.to_csv(src_full_file, index=False)
    input_hydro_table.to_csv(hydro_table_file, index=False)


def reset_hydro_and_src(fim_dir):
    hucs = [h for h in os.listdir(fim_dir) if re.match(r'^\d{8}$', h)]
    for huc_folder in hucs:
        huc_path = os.path.join(fim_dir, huc_folder)
        if os.path.isdir(huc_path):
            for branch_folder in os.listdir(huc_path):
                branch_path = os.path.join(huc_path, branch_folder)
                if os.path.isdir(branch_path):
                    for branch in os.listdir(branch_path):
                        sub_branch_path = os.path.join(branch_path, branch)
                        if os.path.isdir(sub_branch_path):
                            process_branch(sub_branch_path, branch)


# Example usage:
# reset_hydro_and_src('/path/to/fim_dir')
if __name__ == '__main__':
    '''
    Sample usage (min params):
        python3 src/update_htable_src.py
            -d /data/previous_fim/fim_4_5_2_0
    '''
    parser = argparse.ArgumentParser(description='Update hydrotable and src files.')
    parser.add_argument('-d', '--fim_dir', help='Directory path for fim_pipeline output.', required=True)

    args = parser.parse_args()

    reset_hydro_and_src(args.fim_dir)
