#!/usr/bin/env python3

import os

from generate_categorical_fim_mapping import post_process_huc_level, reformat_inundation_maps


fim_run_dir = '/emily_catfim_folders'
huc = '19020401'
job_number_tif = 1
huc_dir = '/alaska_catfim/outputs/catfim_BED_test2_flow_based/mapping/19020401'
attributes_dir = '/alaska_catfim/outputs/catfim_BED_test2_flow_based/attributes/'
gpkg_dir = '/alaska_catfim/outputs/catfim_BED_test2_flow_based/mapping/gpkg'
ahps_dir_list = os.listdir(huc_dir)


fim_version = 'test'
ahps_lid = 'chra2'
extent_grid = (
    '/alaska_catfim/outputs/catfim_BED_test2_flow_based/mapping/19020401/chra2/chra2_record_extent.tif'
)
magnitude = 'record'
nws_lid_attributes_filename = (
    '/alaska_catfim/outputs/catfim_BED_test2_flow_based/attributes/chra2_attributes.csv'
)
interval_stage = (None,)

print('About to run...')

# reformat_inundation_maps(ahps_lid, extent_grid,
#     gpkg_dir, fim_version, huc, magnitude, nws_lid_attributes_filename,
#     interval_stage=None,)


post_process_huc_level(job_number_tif, ahps_dir_list, huc_dir, attributes_dir, gpkg_dir, fim_version, huc)


print('Finished running!')
