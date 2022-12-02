#!/usr/bin/env python3

import os
from posixpath import dirname
import re
import geopandas as gpd
import pandas as pd
import argparse
import warnings
from utils.shared_functions import mem_profile
warnings.simplefilter("ignore")

class Gage2Branch(object):

    def __init__(self, usgs_gage_filename, ahps_filename, huc8):

        self.usgs_gage_filename = usgs_gage_filename
        self.ahps_filename = ahps_filename
        self.huc8 = huc8
        self.load_gages()

    def load_gages(self):

        # Filter USGS gages to huc
        usgs_gages = gpd.read_file(self.usgs_gage_filename)
        self.gages = usgs_gages[(usgs_gages.HUC8 == self.huc8) & (usgs_gages.curve == 'yes')]
        
        # Filter USGS gages by attribute codes
         #https://help.waterdata.usgs.gov/code/coord_acy_cd_query?fmt=html
        acceptable_coord_acc_code_list = ['H','1','5','S','R','B','C','D','E']
        #https://help.waterdata.usgs.gov/code/coord_meth_cd_query?fmt=html
        acceptable_coord_method_code_list = ['C','D','W','X','Y','Z','N','M','L','G','R','F','S']
        #https://help.waterdata.usgs.gov/codes-and-parameters/codes#SI
        acceptable_alt_acc_thresh = 1.0
        #https://help.waterdata.usgs.gov/code/alt_meth_cd_query?fmt=html
        acceptable_alt_meth_code_list = ['A','D','F','I','J','L','N','R','W','X','Y','Z']
        #https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html
        acceptable_site_type_list = ['ST']
        
        self.gages = self.gages.astype({'usgs_data_alt_accuracy_code': 'float'})  # Recast to float
        
        # Filter gages by accuracy codes
        self.gages = self.gages[self.gages['usgs_data_coord_accuracy_code'].isin(acceptable_coord_acc_code_list)]
        self.gages = self.gages[self.gages['usgs_data_coord_method_code'].isin(acceptable_coord_method_code_list)]
        self.gages = self.gages[self.gages['usgs_data_alt_accuracy_code'] <= acceptable_alt_acc_thresh]
        self.gages = self.gages[self.gages['usgs_data_alt_method_code'].isin(acceptable_alt_meth_code_list)]
        self.gages = self.gages[self.gages['usgs_data_site_type'].isin(acceptable_site_type_list)]
        
        # Add accuracy tolerance codes as extra fields so users can know what was used to filter
        self.gages['accepted_usgs_data_coord_accuracy_codes'] = str(acceptable_coord_acc_code_list)
        self.gages['accepted_usgs_data_coord_method_codes'] = str(acceptable_coord_method_code_list)
        self.gages['accepted_usgs_data_alt_accuracy_threshold'] = acceptable_alt_acc_thresh
        self.gages['accepted_usgs_data_alt_meth_codes'] = str(acceptable_alt_meth_code_list)
        self.gages['accepted_usgs_data_site_types'] = str(acceptable_site_type_list)
        
        # Get AHPS sites within the HUC and add them to the USGS dataset
        if self.ahps_filename:
            ahps_sites = gpd.read_file(self.ahps_filename)
            ahps_sites = ahps_sites[ahps_sites.HUC8 == self.huc8] # filter to HUC8
            ahps_sites.rename(columns={'nwm_feature_id':'feature_id',
                                'usgs_site_code':'location_id'}, inplace=True)
            ahps_sites = ahps_sites[ahps_sites.location_id.isna()] # Filter sites that are already in the USGS dataset
            self.gages = self.gages.append(ahps_sites[['feature_id', 'nws_lid', 'location_id', 'HUC8', 'name', 'states','geometry']])

        # Create gages attribute
        self.gages.location_id.fillna(usgs_gages.nws_lid, inplace=True)
        self.gages.loc[self.gages['nws_lid'] == 'Bogus_ID', 'nws_lid'] = None

    def sort_into_branch(self, nwm_subset_streams_levelPaths):

        nwm_reaches = gpd.read_file(nwm_subset_streams_levelPaths)
        nwm_reaches.rename(columns={'ID':'feature_id'}, inplace=True)

        if not self.gages[self.gages.feature_id.isnull()].empty:
            missing_feature_id = self.gages.loc[self.gages.feature_id.isnull()].copy()
            nwm_reaches_union = nwm_reaches.geometry.unary_union
            missing_feature_id['feature_id'] = missing_feature_id.apply(lambda row: self.sjoin_nearest_to_nwm(row.geometry, nwm_reaches, nwm_reaches_union), axis=1)

            self.gages.update(missing_feature_id)

            del nwm_reaches_union

        # Left join gages with NWM streams to get the level path
        self.gages.feature_id = self.gages.feature_id.astype(int)
        self.gages = self.gages.merge(nwm_reaches[['feature_id','levpa_id','order_']], on='feature_id', how='left')
        return self.gages

    def branch_zero(self, bzero_id):

        # note that some gages will not have a valid "order_" attribute (not attributed to a level path in the step before - likely a gage on dropped stream order)
        self.gages.levpa_id = bzero_id
        return self.gages
    
    def write(self, out_name):

        self.gages.to_file(out_name,driver='GPKG',index=False)

    @staticmethod
    def sjoin_nearest_to_nwm(pnt, lines, union):

        snap_geom = union.interpolate(union.project(pnt))
        queried_index = lines.geometry.sindex.query(snap_geom)
        if len(queried_index):
            return int(lines.iloc[queried_index[0]].feature_id.item())

    @staticmethod
    def filter_gage_branches(gms_inputs_filename):

        fim_dir = os.path.dirname(gms_inputs_filename)
        gms_inputs = pd.read_csv(gms_inputs_filename, header=None, names=['huc', 'levpa_id'],
            dtype={'huc':str, 'levpa_id':str})

        for huc_dir in [d for d in os.listdir(fim_dir) if re.search('^\d{8}$', d)]:

            gage_file = os.path.join(fim_dir, huc_dir, 'usgs_subset_gages.gpkg')
            if not os.path.isfile(gage_file):
                gms_inputs.drop(gms_inputs.loc[gms_inputs.huc == huc_dir].index, inplace=True)
                continue

            gages = gpd.read_file(gage_file)
            level_paths = gages.levpa_id
            gms_inputs.drop(gms_inputs.loc[(gms_inputs.huc == huc_dir) & (~gms_inputs.levpa_id.isin(level_paths))].index, inplace=True)
        
        gms_inputs.to_csv(gms_inputs_filename, index=False, header=False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Assign HUC gages to branch and stage for usgs_gage_crosswalk.py')
    parser.add_argument('-gages','--usgs-gages-filename', help='USGS gages', required=True)
    parser.add_argument('-ahps','--nws-lid-filename', help='AHPS gages', required=False)
    parser.add_argument('-nwm','--input-nwm-filename', help='NWM stream subset', required=True)
    parser.add_argument('-o','--output-filename', help='Table to append data', required=True)
    parser.add_argument('-huc','--huc8-id', help='HUC8 ID (to verify gage location huc)', type=str, required=True)
    parser.add_argument('-bzero','--branch-zero-check', help='Check for determining if branch zero is created', type=int, required=True)
    parser.add_argument('-bzero_id','--branch-zero-id', help='Branch zero ID value', type=str, required=True)
    parser.add_argument('-ff','--filter-gms-inputs', help='WARNING: only run this parameter if you know exactly what you are doing', required=False)

    args = vars(parser.parse_args())

    usgs_gages_filename = args['usgs_gages_filename']
    nws_lid_filename = args['nws_lid_filename']
    input_nwm_filename = args['input_nwm_filename']
    output_filename = args['output_filename']
    huc8 = args['huc8_id']
    bzero_check = args['branch_zero_check']
    bzero_id = args['branch_zero_id']
    filter_gms_inputs = args['filter_gms_inputs']

    if not filter_gms_inputs:

        usgs_gage_subset = Gage2Branch(usgs_gages_filename, nws_lid_filename, huc8)
        if usgs_gage_subset.gages.empty:
            print(f'There are no gages identified for {huc8}')
            os._exit(0)
        usgs_gage_subset.sort_into_branch(input_nwm_filename)
        usgs_gage_subset.write(output_filename)

        # Create seperate output for branch zero
        if bzero_check != 0:
            output_filename_zero = os.path.splitext(output_filename)[0] + '_' + bzero_id + os.path.splitext(output_filename)[-1]
            usgs_gage_subset.branch_zero(bzero_id)
            usgs_gage_subset.write(output_filename_zero)

    else:
        ''' 
        This is an easy way to filter gms_inputs so that only branches with gages will run during gms_run_branch.sh.
        You can run this option after gms_run_unit.sh has completed.

        example:
        python3 src/usgs_gage_unit_setup.py -gages x -ahps x -nwm x -o x -huc x -ff /data/outputs/test_output/gms_inputs.csv
        '''
        assert os.path.isfile(filter_gms_inputs)
        Gage2Branch.filter_gage_branches(filter_gms_inputs)

    
