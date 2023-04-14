#!/usr/bin/env python3

import os
from os.path import join
import pandas as pd
import re
import argparse

class HucDirectory(object):

    def __init__(self, path, limit_branches=[]):

        self.dir = path
        self.name = os.path.basename(path)
        self.limit_branches = limit_branches

        self.usgs_dtypes = {'location_id':str,
                            'nws_lid':str,
                            'feature_id':int,
                            'HydroID':int,
                            'levpa_id':str,
                            'dem_elevation':float,
                            'dem_adj_elevation':float,
                            'order_':str,
                            'LakeID':object,
                            'HUC8':str,
                            'snap_distance':float}
        self.agg_usgs_elev_table = pd.DataFrame(columns=list(self.usgs_dtypes.keys()))

        self.hydrotable_dtypes = {'HydroID':int,
                                  'branch_id':int,
                                  'feature_id':int,
                                  'NextDownID':int,
                                  'order_':int,
                                  'Number of Cells':int,
                                  'SurfaceArea (m2)':float,
                                  'BedArea (m2)':float,
                                  'TopWidth (m)':float,
                                  'LENGTHKM':float,
                                  'AREASQKM':float,
                                  'WettedPerimeter (m)':float,
                                  'HydraulicRadius (m)':float,
                                  'WetArea (m2)':float,
                                  'Volume (m3)':float,
                                  'SLOPE':float,
                                  'ManningN':float,
                                  'stage':float,
                                  'default_discharge_cms':float,
                                  'default_Volume (m3)':float,
                                  'default_WetArea (m2)':float,
                                  'default_HydraulicRadius (m)':float,
                                  'default_ManningN':float,
                                  'calb_applied':bool,
                                  'last_updated':str,
                                  'submitter':str,
                                  'obs_source':str,
                                  'precalb_discharge_cms':float,
                                  'calb_coef_usgs':float,
                                  'calb_coef_spatial':float,
                                  'calb_coef_final':float,
                                  'HUC':int,
                                  'LakeID':int,
                                  'subdiv_applied':bool,
                                  'channel_n':float,
                                  'overbank_n':float,
                                  'subdiv_discharge_cms':float,
                                  'discharge_cms':float}
        self.agg_hydrotable = pd.DataFrame(columns=list(self.hydrotable_dtypes.keys()))

        self.src_crosswalked_dtypes = {'branch_id':int,
                                    'HydroID':int,
                                    'feature_id':int,
                                    'Stage':float,
                                    'Number of Cells':int,
                                    'SurfaceArea (m2)':float,
                                    'BedArea (m2)':float,
                                    'Volume (m3)':float,
                                    'SLOPE':float,
                                    'LENGTHKM':float,
                                    'AREASQKM':float,
                                    'ManningN':float,
                                    'NextDownID':int,
                                    'order_':int,
                                    'TopWidth (m)':float,
                                    'WettedPerimeter (m)':float,
                                    'WetArea (m2)':float,
                                    'HydraulicRadius (m)':float,
                                    'Discharge (m3s-1)':float,
                                    'bankfull_flow':float,
                                    'Stage_bankfull':float,
                                    'BedArea_bankfull':float,
                                    'Volume_bankfull':float,
                                    'HRadius_bankfull':float,
                                    'SurfArea_bankfull':float,
                                    'bankfull_proxy':str,
                                    'Volume_chan (m3)':float,
                                    'BedArea_chan (m2)':float,
                                    'WettedPerimeter_chan (m)':float,
                                    'Volume_obank (m3)':float,
                                    'BedArea_obank (m2)':float,
                                    'WettedPerimeter_obank (m)':float,
                                    'channel_n':float,
                                    'overbank_n':float,
                                    'subdiv_applied':bool,
                                    'WetArea_chan (m2)':float,
                                    'HydraulicRadius_chan (m)':float,
                                    'Discharge_chan (m3s-1)':float,
                                    'Velocity_chan (m/s)':float,
                                    'WetArea_obank (m2)':float,
                                    'HydraulicRadius_obank (m)':float,
                                    'Discharge_obank (m3s-1)':float,
                                    'Velocity_obank (m/s)':float,
                                    'Discharge (m3s-1)_subdiv':float}
        self.agg_src_cross = pd.DataFrame(columns=list(self.src_crosswalked_dtypes.keys()))

    def iter_branches(self):

        if self.limit_branches:
            for branch in self.limit_branches:
                yield (branch, join(self.dir, 'branches', branch))

        else:
            for branch in os.listdir(join(self.dir, 'branches')):
                yield (branch, join(self.dir, 'branches', branch))

    def usgs_elev_table(self, branch_path):

        usgs_elev_filename = join(branch_path, 'usgs_elev_table.csv')
        if not os.path.isfile(usgs_elev_filename):
            return

        usgs_elev_table = pd.read_csv(usgs_elev_filename, dtype=self.usgs_dtypes)
        self.agg_usgs_elev_table = self.agg_usgs_elev_table.append(usgs_elev_table)


    def aggregate_hydrotables(self, branch_path, branch_id):

        hydrotable_filename = join(branch_path, f'hydroTable_{branch_id}.csv')
        if not os.path.isfile(hydrotable_filename):
            return

        hydrotable = pd.read_csv(hydrotable_filename, dtype=self.hydrotable_dtypes)
        hydrotable['branch_id'] = branch_id
        hydrotable[['calb_applied']] = hydrotable[['calb_applied']].fillna(value=False)
        self.agg_hydrotable = self.agg_hydrotable.append(hydrotable)

    def aggregate_src_full_crosswalk(self, branch_path, branch_id):

        src_cross_filename = join(branch_path, f'src_full_crosswalked_{branch_id}.csv')
        if not os.path.isfile(src_cross_filename):
            return

        src_cross = pd.read_csv(src_cross_filename, dtype=self.src_crosswalked_dtypes)
        src_cross['branch_id'] = branch_id
        self.agg_src_cross = self.agg_src_cross.append(src_cross)


    def agg_function(self,usgs_elev_flag,hydro_table_flag,src_cross_flag):

        for branch_id, branch_path in self.iter_branches():
            if usgs_elev_flag:
                self.usgs_elev_table(branch_path)

            ## Other aggregate funtions can go here
            if hydro_table_flag:
                self.aggregate_hydrotables(branch_path, branch_id)
            if src_cross_flag:
                self.aggregate_src_full_crosswalk(branch_path, branch_id)

        ## After all of the branches are visited, the code below will write the aggregates
        if usgs_elev_flag:
            usgs_elev_table_file = join(self.dir, 'usgs_elev_table.csv')
            if os.path.isfile(usgs_elev_table_file):
                os.remove(usgs_elev_table_file)

            if not self.agg_usgs_elev_table.empty:
                self.agg_usgs_elev_table.to_csv(usgs_elev_table_file, index=False)

        if hydro_table_flag:
            hydrotable_file = join(self.dir, 'hydrotable.csv')
            if os.path.isfile(hydrotable_file):
                os.remove(hydrotable_file)

            if not self.agg_hydrotable.empty:
                self.agg_hydrotable.to_csv(hydrotable_file, index=False)

        if src_cross_flag:
            src_crosswalk_file = join(self.dir, 'src_full_crosswalked.csv')
            if os.path.isfile(src_crosswalk_file):
                os.remove(src_crosswalk_file)

            if not self.agg_src_cross.empty:
                self.agg_src_cross.to_csv(src_crosswalk_file, index=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregates usgs_elev_table.csv at the HUC level')
    parser.add_argument('-fim','--fim_directory', help='Input FIM Directory', required=True)
    parser.add_argument('-i','--fim_inputs', help='Input fim_inputs CSV file', required=False)
    parser.add_argument('-elev','--usgs_elev', help='Perform aggregate on branch usgs elev tables', required=False, default=False, action='store_true')
    parser.add_argument('-htable','--hydro_table', help='Perform aggregate on branch hydrotables', required=False, default=False, action='store_true')
    parser.add_argument('-src','--src_cross', help='Perform aggregate on branch src crosswalk files', required=False, default=False, action='store_true')

    args = vars(parser.parse_args())

    fim_directory = args['fim_directory']
    fim_inputs = args['fim_inputs']
    usgs_elev_flag = args['usgs_elev']
    hydro_table_flag = args['hydro_table']
    src_cross_flag = args['src_cross']
    assert os.path.isdir(fim_directory), f'{fim_directory} is not a valid directory'

    if fim_inputs:
        fim_inputs = pd.read_csv(fim_inputs, header=None, names=['huc', 'levpa_id'],dtype=str)

        for huc in fim_inputs.huc.unique():

            branches = fim_inputs.loc[fim_inputs.huc == huc, 'levpa_id'].tolist()
            huc = HucDirectory(join(fim_directory, huc), limit_branches=branches)
            huc.agg_function(usgs_elev_flag,hydro_table_flag,src_cross_flag)

    else:
        for huc_dir in [d for d in os.listdir(fim_directory) if re.match('\d{8}', d)]:

            huc = HucDirectory(join(fim_directory, huc_dir))
            huc.agg_function(usgs_elev_flag,hydro_table_flag,src_cross_flag)