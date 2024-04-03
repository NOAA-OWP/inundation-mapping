#!/usr/bin/env python3

import argparse
import os
import shutil

import geopandas as gpd
import pandas as pd


gpd.options.io_engine = "pyogrio"


def update_benchmark_flows(fim_dir: str, output_dir_base: str):
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
        levelpaths: gpd.GeoDataFrame, domain: gpd.GeoDataFrame, flows: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Update benchmark flows of stream segments missing from the flow file

        Parameters
        ----------
        levelpaths : gpd.GeoDataFrame
            Level paths of the HUC
        domain : gpd.GeoDataFrame
            Domain of the site
        flows : pd.DataFrame
            Benchmark flows

        Returns
        -------
        pd.DataFrame
            Flows with updated benchmark flows
        """

        # Intersect levelpaths with the domain to get feature_ids
        levelpaths = gpd.sjoin(levelpaths, domain, how="inner", predicate="intersects")

        if levelpaths.empty:
            return flows

        # Find the levelpath that has flows in the flow file
        levelpath = levelpaths.loc[levelpaths['ID'].isin(flows['feature_id']), 'levpa_id']

        if levelpath.empty:
            return flows

        levelpath = levelpath.values[0]

        levelpaths = levelpaths[levelpaths['levpa_id'] == levelpath]
        IDs = levelpaths.loc[levelpaths['levpa_id'] == levelpath, 'ID']

        # Find IDs not in the flow file
        IDs_to_add = levelpaths.loc[~IDs.isin(flows['feature_id']), 'ID']

        # Add the missing IDs with flows to the flow file
        flows_out = pd.concat(
            [flows, pd.DataFrame({'feature_id': IDs_to_add, 'discharge': flows.loc[0, 'discharge']})],
            ignore_index=True,
        )

        return flows_out

    for org in ['nws', 'usgs']:
        print('Processing', org)

        count_total = 0
        count_updated = 0

        base_dir = f'/data/test_cases/{org}_test_cases/validation_data_{org}'

        huc8s = next(os.walk(base_dir))[1]

        for huc8 in huc8s:
            lids = next(os.walk(f'{base_dir}/{huc8}'))[1]

            for lid in lids:
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

                    if not os.path.exists(flow_file_in):
                        continue

                    flows = pd.read_csv(flow_file_in)

                    flows_new = iterate_over_sites(levelpaths, domain, flows)

                    if len(flows_new) > len(flows):
                        # Backup original flow file
                        backup_flow_file = flow_file_in + '.bak'
                        if not os.path.exists(backup_flow_file):
                            shutil.copy2(flow_file_in, backup_flow_file)

                        flows_new.to_csv(flow_file_out, index=False)

                        count_updated += 1
                        count_total += 1
                    else:
                        count_total += 1

        print(f'Updated {count_updated} out of {count_total} flow files for {org}')


if __name__ == "__main__":
    example_text = '''example:

  %(prog)s -f /outputs/dev-4.4.11.0 -o /outputs/temp
  \n'''

    parser = argparse.ArgumentParser(
        epilog=example_text, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-f', '--fim_dir', help='Location of FIM files', type=str, required=True)
    parser.add_argument('-o', '--output_dir-base', help='Output directory', type=str, default=None)
    args = vars(parser.parse_args())

    update_benchmark_flows(**args)
