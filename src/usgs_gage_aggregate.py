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


    def agg_function(self):

        for branch_id, branch_path in self.iter_branches():

            self.usgs_elev_table(branch_path)

            ## Other aggregate funtions can go here
        
        ## After all of the branches are visited, the code below will write the aggregates
        if os.path.isfile(join(self.dir, 'usgs_elev_table.csv')):
            os.remove(join(self.dir, 'usgs_elev_table.csv'))

        if not self.agg_usgs_elev_table.empty:
            self.agg_usgs_elev_table.to_csv(join(self.dir, 'usgs_elev_table.csv'), index=False)
        


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregates usgs_elev_table.csv at the HUC level')
    parser.add_argument('-fim','--fim_directory', help='Input FIM Directory', required=True)
    parser.add_argument('-gms','--gms_inputs', help='Input gms_inputs CSV file', required=False)

    args = vars(parser.parse_args())

    fim_directory = args['fim_directory']
    gms_inputs = args['gms_inputs']
    assert os.path.isdir(fim_directory), f'{fim_directory} is not a valid directory'

    if gms_inputs:
        gms_inputs = pd.read_csv(gms_inputs, header=None, names=['huc', 'levpa_id'],dtype=str)

        for huc in gms_inputs.huc.unique():

            branches = gms_inputs.loc[gms_inputs.huc == huc, 'levpa_id'].tolist()
            huc = HucDirectory(join(fim_directory, huc), limit_branches=branches)
            huc.agg_function()

    else:
        for huc_dir in [d for d in os.listdir(fim_directory) if re.match('\d{8}', d)]:

            huc = HucDirectory(join(fim_directory, huc_dir))
            huc.agg_function()


        



