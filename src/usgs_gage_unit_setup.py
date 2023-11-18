#!/usr/bin/env python3

import argparse
import os
import re
import warnings
from posixpath import dirname

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from utils.shared_variables import PREP_CRS


warnings.simplefilter("ignore")


class Gage2Branch(object):
    def __init__(self, usgs_gage_filename, ras_locs_filename, ahps_filename, huc8):
        self.usgs_gage_filename = usgs_gage_filename
        self.ras_locs_filename = ras_locs_filename
        self.ahps_filename = ahps_filename
        self.huc8 = str(huc8)
        self.load_gages()

    def load_gages(self):
        # Read USGS gage file a
        usgs_gages = gpd.read_file(self.usgs_gage_filename, dtype={'location_id': object})
        usgs_gages['source'] = 'usgs_gage'

        # Read RAS2FIM point locations file
        # !!! Geopandas is not honoring the dtype arg with this read_file below (huc8 being read as int64).
        # Need the raw data to store the 'huc8' attribute as an object to avoid issues with integers truncating the leading zero from some hucs
        ras_locs = gpd.read_file(self.ras_locs_filename, dtype={'huc8': 'object'})
        ras_locs = ras_locs[['feature_id', 'huc8', 'stream_stn', 'fid_xs', 'source', 'geometry']]
        ras_locs['location_id'] = ras_locs['fid_xs']

        # Convert ras locs crs to match usgs gage crs
        ras_locs.to_crs(usgs_gages.crs, inplace=True)
        ras_locs = ras_locs.rename(columns={'huc8': 'HUC8'})

        # Convert Multipoint geometry to Point geometry
        ras_locs['geometry'] = ras_locs.representative_point()

        # if ras_locs.huc8.dtype == 'int64':
        #     ras_locs = ras_locs[ras_locs.huc8 == int(self.huc8)]
        #     ras_locs['HUC8'] = str(self.huc8)
        #     ras_locs = ras_locs.drop('huc8', axis=1)
        # elif ras_locs.huc8.dtype == 'int64':
        #     ras_locs = ras_locs.rename(columns={'huc8':'HUC8'})

        # Concat USGS points and RAS2FIM points
        gages_locs = pd.concat([usgs_gages, ras_locs], axis=0, ignore_index=True)
        # gages_locs.to_crs(PREP_CRS, inplace=True)

        # Filter USGS gages and RAS locations to huc
        self.gages = gages_locs[(gages_locs.HUC8 == self.huc8)]

        # Get AHPS sites within the HUC and add them to the USGS dataset
        if self.ahps_filename:
            ahps_sites = gpd.read_file(self.ahps_filename)
            ahps_sites = ahps_sites[ahps_sites.HUC8 == self.huc8]  # filter to HUC8
            ahps_sites = ahps_sites.rename(
                columns={'nwm_feature_id': 'feature_id', 'usgs_site_code': 'location_id'}
            )
            ahps_sites = ahps_sites[
                ahps_sites.location_id.isna()
            ]  # Filter sites that are already in the USGS dataset
            ahps_sites['source'] = 'ahps_site'
            self.gages = pd.concat(
                [
                    self.gages,
                    ahps_sites[
                        ['feature_id', 'nws_lid', 'location_id', 'HUC8', 'name', 'states', 'geometry']
                    ],
                ]
            )

        # Create gages attribute
        self.gages.location_id.fillna(self.gages.nws_lid, inplace=True)
        self.gages.loc[self.gages['nws_lid'] == 'Bogus_ID', 'nws_lid'] = None

    def sort_into_branch(self, nwm_subset_streams_levelPaths):
        nwm_reaches = gpd.read_file(nwm_subset_streams_levelPaths)
        nwm_reaches = nwm_reaches.rename(columns={'ID': 'feature_id'})

        if not self.gages[self.gages.feature_id.isnull()].empty:
            missing_feature_id = self.gages.loc[self.gages.feature_id.isnull()].copy()
            nwm_reaches_union = nwm_reaches.geometry.unary_union
            missing_feature_id['feature_id'] = missing_feature_id.apply(
                lambda row: self.sjoin_nearest_to_nwm(row.geometry, nwm_reaches, nwm_reaches_union), axis=1
            )

            self.gages.update(missing_feature_id)
            self.gages = self.gages.set_geometry("geometry")

            del nwm_reaches_union

        # Left join gages with NWM streams to get the level path
        self.gages.feature_id = self.gages.feature_id.astype(int)
        self.gages = self.gages.merge(
            nwm_reaches[['feature_id', 'levpa_id', 'order_']], on='feature_id', how='left'
        )
        return self.gages

    def branch_zero(self, bzero_id):
        # note that some gages will not have a valid "order_" attribute
        #   (not attributed to a level path in the step before - likely a gage on dropped stream order)
        self.gages.levpa_id = bzero_id
        return self.gages

    def write(self, out_name):
        self.gages.to_file(out_name, driver='GPKG', index=False)

    @staticmethod
    def sjoin_nearest_to_nwm(pnt, lines, union):
        snap_geom = union.interpolate(union.project(pnt))
        queried_index = lines.geometry.sindex.query(snap_geom)
        if len(queried_index):
            return int(lines.iloc[queried_index[0]].feature_id.item())

    @staticmethod
    def filter_gage_branches(fim_inputs_filename):
        fim_dir = os.path.dirname(fim_inputs_filename)
        fim_inputs = pd.read_csv(
            fim_inputs_filename, header=None, names=['huc', 'levpa_id'], dtype={'huc': str, 'levpa_id': str}
        )

        for huc_dir in [d for d in os.listdir(fim_dir) if re.search(r'^\d{8}$', d)]:
            gage_file = os.path.join(fim_dir, huc_dir, 'usgs_subset_gages.gpkg')
            if not os.path.isfile(gage_file):
                fim_inputs = fim_inputs.drop(fim_inputs.loc[fim_inputs.huc == huc_dir].index)
                continue

            gages = gpd.read_file(gage_file)
            level_paths = gages.levpa_id
            fim_inputs = fim_inputs.drop(
                fim_inputs.loc[(fim_inputs.huc == huc_dir) & (~fim_inputs.levpa_id.isin(level_paths))].index
            )

        fim_inputs.to_csv(fim_inputs_filename, index=False, header=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Assign HUC gages to branch and stage for usgs_gage_crosswalk.py'
    )
    parser.add_argument('-gages', '--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-ras', '--ras-locs-filename', help='RAS2FIM rating curve locations', required=True)
    parser.add_argument('-ahps', '--nws-lid-filename', help='AHPS gages', required=False)
    parser.add_argument('-nwm', '--input-nwm-filename', help='NWM stream subset', required=True)
    parser.add_argument('-o', '--output-filename', help='Table to append data', required=True)
    parser.add_argument(
        '-huc', '--huc8-id', help='HUC8 ID (to verify gage location huc)', type=str, required=True
    )
    parser.add_argument('-bzero_id', '--branch-zero-id', help='Branch zero ID value', type=str, required=True)
    parser.add_argument(
        '-ff',
        '--filter-fim-inputs',
        help='WARNING: only run this parameter if you know exactly what you are doing',
        required=False,
    )

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    ras_locs_filename = args['ras_locs_filename']
    nws_lid_filename = args['nws_lid_filename']
    input_nwm_filename = args['input_nwm_filename']
    output_filename = args['output_filename']
    huc8 = args['huc8_id']
    bzero_id = args['branch_zero_id']
    filter_fim_inputs = args['filter_fim_inputs']

    if not filter_fim_inputs:
        usgs_gage_subset = Gage2Branch(usgs_gages_filename, ras_locs_filename, nws_lid_filename, huc8)
        if usgs_gage_subset.gages.empty:
            print(f'There are no gages identified for {huc8}')
            os._exit(0)
        usgs_gage_subset.sort_into_branch(input_nwm_filename)
        usgs_gage_subset.write(output_filename)

        # Create seperate output for branch zero
        output_filename_zero = (
            os.path.splitext(output_filename)[0] + '_' + bzero_id + os.path.splitext(output_filename)[-1]
        )
        usgs_gage_subset.branch_zero(bzero_id)
        usgs_gage_subset.write(output_filename_zero)

    else:
        '''
        This is an easy way to filter fim_inputs so that only branches with gages will run
        during fim_process_unit_wb.sh.

        example:
        python3 src/usgs_gage_unit_setup.py -gages x -ahps x -nwm x -o x -huc x
            -ff /outputs/test_output/fim_inputs.csv
        '''
        assert os.path.isfile(filter_fim_inputs)
        Gage2Branch.filter_gage_branches(filter_fim_inputs)
