#!/usr/bin/env python3

import argparse
import os
import shutil

import geopandas as gpd
import pandas as pd


gpd.options.io_engine = "pyogrio"


def update_benchmark_flows(fim_dir: str, output_dir_base: str, verbose: bool = False):
    """
    Update benchmark flows of the levelpath in the domain for stream segments missing from the flow file

    Parameters
    ----------
    fim_dir : str
        Location of FIM files (e.g., "/outputs/dev-4.4.11.0")
    output_dir_base : str
        Output directory (e.g., "/outputs/temp"). If None, the output files will be saved in the input directory (the original files will be saved with a ".bak" extension appended to the original filename).
    """

    def iterate_over_sites(
        levelpaths: gpd.GeoDataFrame, levelpaths_domain: gpd.GeoDataFrame, flows: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Update benchmark flows of stream segments missing from the flow file

        Parameters
        ----------
        levelpaths : gpd.GeoDataFrame
            Level paths of the HUC
        levelpaths_domain : gpd.GeoDataFrame
            Levelpaths in the domain
        flows : pd.DataFrame
            Benchmark flows

        Returns
        -------
        pd.DataFrame
            Flows with updated benchmark flows
        """

        if levelpaths_domain.empty:
            return flows

        # Find the levelpaths with the highest order (to exclude tributaries)
        levelpaths_domain = levelpaths_domain[
            levelpaths_domain['order_'] == levelpaths_domain['order_'].max()
        ]

        # Find the levelpath that has flows in the flow file
        if levelpaths_domain['levpa_id'].nunique() > 1:
            # If there are multiple levelpaths with the highest order, take the longest one (intersect levelpaths_domain with the domain to get the length of the levelpath)
            levelpaths_domain_intersect = gpd.overlay(levelpaths, domain, how='intersection')
            levelpaths_domain_intersect['length'] = levelpaths_domain_intersect['geometry'].length

            # Get the total length of all the segments in the levelpath
            levelpaths_domain_intersect_length = (
                levelpaths_domain_intersect.groupby('levpa_id').agg({'length': 'sum'}).reset_index()
            )

            # Get the longest levelpath
            levelpath = levelpaths_domain_intersect_length.loc[
                levelpaths_domain_intersect_length['length'].idxmax(), 'levpa_id'
            ]

        else:
            levelpath = levelpaths_domain['levpa_id'].iloc[0]

        # Get IDs of all features in the levelpath
        IDs_to_keep = levelpaths[levelpaths['levpa_id'] == levelpath]['ID']

        # Get IDs of all features in the levelpath in the domain
        IDs = levelpaths_domain.loc[levelpaths_domain['levpa_id'] == levelpath, 'ID']

        # Keep the flows that are in the levelpath (remove tributaries)
        flows = flows.iloc[flows.loc[flows['feature_id'].isin(IDs_to_keep)].index]

        if flows.empty:
            return flows

        # Find IDs not in the flow file
        add_IDs = ~IDs.isin(flows['feature_id'])

        # Exit if no IDs to add
        if not any(add_IDs):
            return flows

        IDs_to_add = levelpaths_domain.loc[add_IDs.index, 'ID']

        # Add the missing IDs with flows to the flow file
        if not IDs_to_add.empty:
            flows_out = pd.concat(
                [flows, pd.DataFrame({'feature_id': IDs_to_add, 'discharge': flows['discharge'].iloc[0]})],
                ignore_index=True,
            )
        else:
            flows_out = flows

        return flows_out

    for org in ['nws', 'usgs']:
        if verbose:
            print('Processing', org)

        count_total = 0
        count_updated = 0

        base_dir = f'/data/test_cases/{org}_test_cases/validation_data_{org}'

        huc8s = next(os.walk(base_dir))[1]

        for huc8 in huc8s:
            if verbose:
                print(f'\t{huc8}')

            lids = next(os.walk(f'{base_dir}/{huc8}'))[1]

            for lid in lids:
                if verbose:
                    print(f'\t\t{lid}')

                # Read the input files
                levelpath_file = f'{fim_dir}/{huc8}/nwm_subset_streams_levelPaths.gpkg'
                if not os.path.exists(levelpath_file):
                    continue

                levelpaths = gpd.read_file(levelpath_file)

                validation_path = f'{base_dir}/{huc8}/{lid}'

                domain_file = f'{validation_path}/{lid}_domain.shp'
                if not os.path.exists(domain_file):
                    continue

                domain = gpd.read_file(domain_file)

                # Intersect levelpaths with the domain to get feature_ids
                levelpaths_domain = gpd.sjoin(levelpaths, domain, how="inner", predicate="intersects")

                magnitudes = next(os.walk(validation_path))[1]

                for magnitude in magnitudes:
                    input_dir = f'{validation_path}/{magnitude}'

                    if output_dir_base is None:
                        output_dir = input_dir
                    else:
                        output_dir = os.path.join(output_dir_base, org)

                        if not os.path.exists(output_dir):
                            os.makedirs(output_dir)

                    flow_file_in = f'{input_dir}/ahps_{lid}_huc_{huc8}_flows_{magnitude}.csv'
                    flow_file_out = f'{output_dir}/ahps_{lid}_huc_{huc8}_flows_{magnitude}.csv'

                    # Skip if flow file doesn't exist
                    if not os.path.exists(flow_file_in):
                        continue

                    # Backup original flow file
                    backup_flow_file = flow_file_out + '.bak'
                    if not os.path.exists(backup_flow_file):
                        shutil.copy2(flow_file_in, backup_flow_file)

                    flows = pd.read_csv(backup_flow_file)

                    flows_new = iterate_over_sites(levelpaths, levelpaths_domain, flows)

                    if not flows_new.equals(flows):
                        count_updated += 1
                        count_total += 1
                    else:
                        count_total += 1

                    flows_new.to_csv(flow_file_out, index=False)

        print(f'\tUpdated {count_updated} out of {count_total} flow files for {org}')


if __name__ == "__main__":
    example_text = '''example:

  %(prog)s -f /outputs/dev-4.4.11.0 -o /outputs/temp
  \n'''

    parser = argparse.ArgumentParser(
        epilog=example_text, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-f', '--fim_dir', help='Location of FIM files', type=str, required=True)
    parser.add_argument('-o', '--output_dir-base', help='Output directory', type=str, default=None)
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true')

    args = vars(parser.parse_args())

    update_benchmark_flows(**args)
