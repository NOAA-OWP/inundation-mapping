#!/usr/bin/env python3

import argparse
import os
import shutil

import geopandas as gpd
import pandas as pd


gpd.options.io_engine = "pyogrio"


def update_benchmark_flows(version):
    """
    Update benchmark flows of the levelpath in the domain for stream segments missing from the flow file

    Parameters
    ----------
    version : str
        FIM version
    """

    def iterate_over_BAD_SITES(
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

    BAD_SITES = [
        ['cpei3', '04050001', 'USGS'],
        ['hohn4', '02030103', 'Both'],
        ['kilo1', '05040003', 'Both'],
        ['monv1', '04300103', 'NWS'],
        ['nhso1', '05040001', 'USGS'],
        ['nmso1', '05040006', 'Both'],
        ['pori3', '05120102', 'USGS'],
        ['ptvn6', '02020005', 'Both'],
        ['selt2', '12100304', 'NWS'],
        ['sweg1', '03130001', 'Both'],
        ['watw3', '07090001', 'NWS'],
        ['weat2', '12030102', 'NWS'],
        ['wkew3', '07120006', 'NWS'],
    ]

    for lid, huc8, organization in BAD_SITES:
        if organization == 'USGS':
            orgs = ['usgs']
        elif organization == 'NWS':
            orgs = ['nws']
        else:
            orgs = ['usgs', 'nws']

        # Read the input files
        levelpath_file = f'/outputs/{version}/{huc8}/nwm_subset_streams_levelPaths.gpkg'
        assert os.path.exists(levelpath_file), f"Levelpath file {levelpath_file} does not exist"
        levelpaths = gpd.read_file(levelpath_file)

        for org in orgs:
            validation_path = f'/data/test_cases/{org}_test_cases/validation_data_{org}/{huc8}/{lid}'

            domain_file = f'{validation_path}/{lid}_domain.shp'
            assert os.path.exists(domain_file), f"Domain file {domain_file} does not exist"
            domain = gpd.read_file(domain_file)

            magnitudes = next(os.walk(validation_path))[1]

            for magnitude in magnitudes:
                flow_file_in = f'{validation_path}/{magnitude}/ahps_{lid}_huc_{huc8}_flows_{magnitude}.csv'

                assert os.path.exists(flow_file_in), f"Flow file {flow_file_in} does not exist"

                # Backup original flow file
                backup_flow_file = flow_file_in + '.bak'
                if not os.path.exists(backup_flow_file):
                    shutil.copy2(flow_file_in, backup_flow_file)

                flows = pd.read_csv(flow_file_in)

                flows = iterate_over_BAD_SITES(levelpaths, domain, flows)

                flows.to_csv(flow_file_in, index=False)


if __name__ == "__main__":
    example_text = '''example:

  %(prog)s -v dev-4.4.11.0
  \n'''

    parser = argparse.ArgumentParser(
        epilog=example_text, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('-v', '--version', help='Version of the model', type=str, required=True)
    args = vars(parser.parse_args())

    update_benchmark_flows(**args)
