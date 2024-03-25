#!/usr/bin/env python3

import argparse
import os
import shutil

import geopandas as gpd
import pandas as pd


gpd.options.io_engine = "pyogrio"


def update_benchmark_flows(version: str, output_dir_base: str):
    """
    Update benchmark flows of the levelpath in the domain for stream segments missing from the flow file

    Parameters
    ----------
    version : str
        FIM version
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
        flows_out : pd.DataFrame
            Flows with updated benchmark flows
        """

        # Intersect levelpaths with the domain to get feature_ids
        levelpaths = gpd.sjoin(levelpaths, domain, how="inner", predicate="intersects")

        # Find the levelpath that has flows in the flow file
        levelpath = levelpaths.loc[levelpaths['ID'].isin(flows['feature_id']), 'levpa_id'].values[0]

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
        base_dir = f'/data/test_cases/{org}_test_cases/validation_data_{org}'

        huc8s = next(os.walk(base_dir))[1]

        for huc8 in huc8s:
            lids = next(os.walk(f'{base_dir}/{huc8}'))[1]

            for lid in lids:
                # Read the input files
                levelpath_file = f'/outputs/{version}/{huc8}/nwm_subset_streams_levelPaths.gpkg'
                assert os.path.exists(levelpath_file), f"Levelpath file {levelpath_file} does not exist"
                levelpaths = gpd.read_file(levelpath_file)

                validation_path = f'{base_dir}/{huc8}/{lid}'

                domain_file = f'{validation_path}/{lid}_domain.shp'
                assert os.path.exists(domain_file), f"Domain file {domain_file} does not exist"
                domain = gpd.read_file(domain_file)

                magnitudes = next(os.walk(validation_path))[1]

                for magnitude in magnitudes:
                    input_dir = f'{validation_path}/{magnitude}'

                    if output_dir_base is None:
                        output_dir = input_dir
                    elif not os.path.exists(output_dir_base):
                        output_dir = os.path.join(output_dir_base, org)
                        os.makedirs(output_dir)

                    flow_file_in = f'{input_dir}/ahps_{lid}_huc_{huc8}_flows_{magnitude}.csv'
                    flow_file_out = f'{output_dir}/ahps_{lid}_huc_{huc8}_flows_{magnitude}.csv'

                    assert os.path.exists(flow_file_in), f"Flow file {flow_file_in} does not exist"

                    flows = pd.read_csv(flow_file_in)

                    flows_new = iterate_over_sites(levelpaths, domain, flows)

                    if len(flows_new) > len(flows):
                        # Backup original flow file
                        backup_flow_file = flow_file_in + '.bak'
                        if not os.path.exists(backup_flow_file):
                            shutil.copy2(flow_file_in, backup_flow_file)

                        flows_new.to_csv(flow_file_out, index=False)


if __name__ == "__main__":
    example_text = '''example:

  %(prog)s -v dev-4.4.11.0
  \n'''

    parser = argparse.ArgumentParser(
        epilog=example_text, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-v', '--version', help='Version of the model', type=str, required=True)
    parser.add_argument('-o', '--output_dir-base', help='Output directory', type=str, default=None)
    args = vars(parser.parse_args())

    update_benchmark_flows(**args)
