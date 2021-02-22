#!/usr/bin/env python3

import os
import sys
import shutil
import argparse

from utils.shared_functions import compute_contingency_stats_from_rasters
from utils.shared_variables import (TEST_CASES_DIR, INPUTS_DIR, ENDC, TRED_BOLD, WHITE_BOLD, CYAN_BOLD, AHPS_BENCHMARK_CATEGORIES)
from inundation import inundate


def run_alpha_test(fim_run_dir, version, test_id, magnitude, compare_to_previous=False, archive_results=False, mask_type='huc', inclusion_area='', inclusion_area_buffer=0, light_run=False, overwrite=True):
    
    benchmark_category = test_id.split('_')[1] # Parse benchmark_category from test_id.
    current_huc = test_id.split('_')[0]  # Break off HUC ID and assign to variable.
    
    # Construct paths to development test results if not existent.
    if archive_results:
        version_test_case_dir_parent = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', test_id, 'official_versions', version)
    else:
        version_test_case_dir_parent = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', test_id, 'testing_versions', version)

    # Delete the entire directory if it already exists.
    if os.path.exists(version_test_case_dir_parent):
        if overwrite == True:
            shutil.rmtree(version_test_case_dir_parent)
        else:
            print("Metrics for ({version}: {test_id}) already exist. Use overwrite flag (-o) to overwrite metrics.".format(version=version, test_id=test_id))
            return
        
    os.mkdir(version_test_case_dir_parent)

    print("Running the alpha test for test_id: " + test_id + ", " + version + "...")
    stats_modes_list = ['total_area']

    fim_run_parent = os.path.join(os.environ['outputDataDir'], fim_run_dir)
    assert os.path.exists(fim_run_parent), "Cannot locate " + fim_run_parent

    # Create paths to fim_run outputs for use in inundate().
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    if not os.path.exists(rem):
        rem = os.path.join(fim_run_parent, 'rem_clipped_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    if not os.path.exists(catchments):
        catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_clipped_addedAttributes.tif')
    if mask_type == 'huc':
        catchment_poly = ''
    else:
        catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')
        
    # Map necessary inputs for inundation().
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'

    # Create list of shapefile paths to use as exclusion areas.
    zones_dir = os.path.join(TEST_CASES_DIR, 'other', 'zones')
    mask_dict = {'levees': 
                    {'path': os.path.join(zones_dir, 'leveed_areas_conus.shp'),
                     'buffer': None,
                     'operation': 'exclude'
                     },
                'waterbodies':
                    {'path': os.path.join(zones_dir, 'nwm_v2_reservoirs.shp'),
                     'buffer': None,
                     'operation': 'exclude',
                     },
                }
            
    if inclusion_area != '':
        inclusion_area_name = os.path.split(inclusion_area)[1].split('.')[0]  # Get layer name
        mask_dict.update({inclusion_area_name: {'path': inclusion_area,
                                                'buffer': int(inclusion_area_buffer),
                                                'operation': 'include'}})
        # Append the concatenated inclusion_area_name and buffer.
        if inclusion_area_buffer == None:
            inclusion_area_buffer = 0
        stats_modes_list.append(inclusion_area_name + '_b' + str(inclusion_area_buffer) + 'm') 

    # Check if magnitude is list of magnitudes or single value.
    magnitude_list = magnitude
    if type(magnitude_list) != list:
        magnitude_list = [magnitude_list]

    # Get path to validation_data_{benchmark} directory and huc_dir.
    validation_data_path = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category)
    
    for magnitude in magnitude_list:
        version_test_case_dir = os.path.join(version_test_case_dir_parent, magnitude)
        if not os.path.exists(version_test_case_dir):
            os.mkdir(version_test_case_dir)
    
        # Construct path to validation raster and forecast file.
        if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
            benchmark_raster_path_list, forecast_list = [], []
            lid_dir_list = os.listdir(os.path.join(validation_data_path, current_huc))
            lid_list, inundation_raster_list, extent_file_list = [], [], []
            
            for lid in lid_dir_list:
                lid_dir = os.path.join(validation_data_path, current_huc, lid)
                benchmark_raster_path_list.append(os.path.join(lid_dir, magnitude, 'ahps_' + lid + '_huc_' + current_huc + '_depth_' + magnitude + '.tif'))  # TEMP
                forecast_list.append(os.path.join(lid_dir, magnitude, 'ahps_' + lid + '_huc_' + current_huc + '_flows_' + magnitude + '.csv'))  # TEMP
                lid_list.append(lid)
                inundation_raster_list.append(os.path.join(version_test_case_dir, lid + '_inundation_extent.tif'))
                extent_file_list.append(os.path.join(lid_dir, lid + '_extent.shp'))
                    
            ahps_inclusion_zones_dir = os.path.join(version_test_case_dir_parent, 'ahps_domains')
            
            if not os.path.exists(ahps_inclusion_zones_dir):
                os.mkdir(ahps_inclusion_zones_dir)

        else:
            benchmark_raster_file = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_depth_' + magnitude + '.tif')
            benchmark_raster_path_list = [benchmark_raster_file]
            forecast_path = os.path.join(TEST_CASES_DIR, benchmark_category + '_test_cases', 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_flows_' + magnitude + '.csv')
            forecast_list = [forecast_path]
            inundation_raster_list = [os.path.join(version_test_case_dir, 'inundation_extent.tif')]
            
        for index in range(0, len(benchmark_raster_path_list)):
            benchmark_raster_path = benchmark_raster_path_list[index]
            forecast = forecast_list[index]
            inundation_raster = inundation_raster_list[index]
            
            # Only need to define ahps_lid and ahps_extent_file for AHPS_BENCHMARK_CATEGORIES.
            if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                ahps_lid = lid_list[index]
                ahps_extent_file = extent_file_list[index]
                mask_dict.update({ahps_lid:
                    {'path': ahps_extent_file,
                     'buffer': None,
                     'operation': 'include'}
                        })
        
                if not os.path.exists(benchmark_raster_path) or not os.path.exists(ahps_extent_file) or not os.path.exists(forecast):  # Skip loop instance if the benchmark raster doesn't exist.
                    continue
            else:  # If not in AHPS_BENCHMARK_CATEGORIES.
                if not os.path.exists(benchmark_raster_path) or not os.path.exists(forecast):  # Skip loop instance if the benchmark raster doesn't exist.
                    continue
    
            # Run inundate.
            print("-----> Running inundate() to produce modeled inundation extent for the " + magnitude + " magnitude...")
            try:
                inundate(
                         rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                         subset_hucs=current_huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                         depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
                        )
            
                print("-----> Inundation mapping complete.")
                predicted_raster_path = os.path.join(os.path.split(inundation_raster)[0], os.path.split(inundation_raster)[1].replace('.tif', '_' + current_huc + '.tif'))  # The inundate adds the huc to the name so I account for that here.
        
                # Define outputs for agreement_raster, stats_json, and stats_csv.
                if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                    agreement_raster, stats_json, stats_csv = os.path.join(version_test_case_dir, lid + 'total_area_agreement.tif'), os.path.join(version_test_case_dir, 'stats.json'), os.path.join(version_test_case_dir, 'stats.csv')
                else:
                    agreement_raster, stats_json, stats_csv = os.path.join(version_test_case_dir, 'total_area_agreement.tif'), os.path.join(version_test_case_dir, 'stats.json'), os.path.join(version_test_case_dir, 'stats.csv')
         
                compute_contingency_stats_from_rasters(predicted_raster_path,
                                                       benchmark_raster_path,
                                                       agreement_raster,
                                                       stats_csv=stats_csv,
                                                       stats_json=stats_json,
                                                       mask_values=[],
                                                       stats_modes_list=stats_modes_list,
                                                       test_id=test_id,
                                                       mask_dict=mask_dict,
                                                       )
        
                if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
                    del mask_dict[ahps_lid]
                
                print(" ")
                print("Evaluation complete. All metrics for " + test_id + ", " + version + ", " + magnitude + " are available at " + CYAN_BOLD + version_test_case_dir + ENDC)
                print(" ")
            except Exception as e:
                print(e)      
        
        if benchmark_category in AHPS_BENCHMARK_CATEGORIES:
            # -- Delete temp files -- #
            # List all files in the output directory.
            output_file_list = os.listdir(version_test_case_dir)
            for output_file in output_file_list:
                if "total_area" in output_file:
                    full_output_file_path = os.path.join(version_test_case_dir, output_file)
                    os.remove(full_output_file_path)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping and regression analysis for FOSS FIM. Regression analysis results are stored in the test directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-b', '--version-name',help='The name of the working version in which features are being tested',required=True,default="")
    parser.add_argument('-t', '--test-id',help='The test_id to use. Format as: HUC_BENCHMARKTYPE, e.g. 12345678_ble.',required=True,default="")
    parser.add_argument('-m', '--mask-type', help='Specify \'huc\' (FIM < 3) or \'filter\' (FIM >= 3) masking method', required=False,default="huc")
    parser.add_argument('-y', '--magnitude',help='The magnitude to run.',required=False, default="")
    parser.add_argument('-c', '--compare-to-previous', help='Compare to previous versions of HAND.', required=False,action='store_true')
    parser.add_argument('-a', '--archive-results', help='Automatically copy results to the "previous_version" archive for test_id. For admin use only.', required=False,action='store_true')
    parser.add_argument('-i', '--inclusion-area', help='Path to shapefile. Contingency metrics will be produced from pixels inside of shapefile extent.', required=False, default="")
    parser.add_argument('-ib','--inclusion-area-buffer', help='Buffer to use when masking contingency metrics with inclusion area.', required=False, default="0")
    parser.add_argument('-l', '--light-run', help='Using the light_run option will result in only stat files being written, and NOT grid files.', required=False, action='store_true')
    parser.add_argument('-o','--overwrite',help='Overwrite all metrics or only fill in missing metrics.',required=False, default=False)

    # Extract to dictionary and assign to variables.
    args = vars(parser.parse_args())

    valid_test_id_list = os.listdir(TEST_CASES_DIR)

    exit_flag = False  # Default to False.
    print()

    # Ensure test_id is valid.
    if args['test_id'] not in valid_test_id_list:
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided test_id (-t) " + CYAN_BOLD + args['test_id'] + WHITE_BOLD + " is not available." + ENDC)
        print(WHITE_BOLD + "Available test_ids include: " + ENDC)
        for test_id in valid_test_id_list:
          if 'validation' not in test_id.split('_') and 'ble' in test_id.split('_'):
              print(CYAN_BOLD + test_id + ENDC)
        print()
        exit_flag = True

    # Ensure fim_run_dir exists.
    if not os.path.exists(os.path.join(os.environ['outputDataDir'], args['fim_run_dir'])):
        print(TRED_BOLD + "Warning: " + WHITE_BOLD + "The provided fim_run_dir (-r) " + CYAN_BOLD + args['fim_run_dir'] + WHITE_BOLD + " could not be located in the 'outputs' directory." + ENDC)
        print(WHITE_BOLD + "Please provide the parent directory name for fim_run.sh outputs. These outputs are usually written in a subdirectory, e.g. outputs/123456/123456." + ENDC)
        print()
        exit_flag = True
    
    # Ensure inclusion_area path exists.
    if args['inclusion_area'] != "" and not os.path.exists(args['inclusion_area']):
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided inclusion_area (-i) " + CYAN_BOLD + args['inclusion_area'] + WHITE_BOLD + " could not be located." + ENDC)
        exit_flag = True
        
    try:
        inclusion_buffer = int(args['inclusion_area_buffer'])
    except ValueError:
        print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided inclusion_area_buffer (-ib) " + CYAN_BOLD + args['inclusion_area_buffer'] + WHITE_BOLD + " is not a round number." + ENDC)

    if args['magnitude'] == '':
        if 'ble' in args['test_id'].split('_'):
            args['magnitude'] = ['100yr', '500yr']
        elif 'ahps' in args['test_id'].split('_'):
            args['magnitude'] = ['action', 'minor', 'moderate', 'major']
        else:
            print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided magnitude (-y) " + CYAN_BOLD + args['magnitude'] + WHITE_BOLD + " is invalid. ble options include: 100yr, 500yr. ahps options include action, minor, moderate, major." + ENDC)
            exit_flag = True     
            
    if exit_flag:
        print()
        sys.exit()

    else:
        run_alpha_test(**args)
