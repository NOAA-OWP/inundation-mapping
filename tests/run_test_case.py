#!/usr/bin/env python3

import os
import sys
import pandas as pd
import geopandas as gpd
import rasterio
import json
import csv
import argparse
import shutil

from utils.shared_functions import get_contingency_table_from_binary_rasters, compute_stats_from_contingency_table
from inundation import inundate

TEST_CASES_DIR = r'/data/test_cases_ahps_testing/'  # Will update.
INPUTS_DIR = r'/data/inputs'
PRINTWORTHY_STATS = ['CSI', 'TPR', 'TNR', 'FAR', 'MCC', 'TP_area_km2', 'FP_area_km2', 'TN_area_km2', 'FN_area_km2', 'contingency_tot_area_km2', 'TP_perc', 'FP_perc', 'TN_perc', 'FN_perc']
GO_UP_STATS = ['CSI', 'TPR', 'MCC', 'TN_area_km2', 'TP_area_km2', 'TN_perc', 'TP_perc', 'TNR']
GO_DOWN_STATS = ['FAR', 'FN_area_km2', 'FP_area_km2', 'FP_perc', 'FN_perc']
OUTPUTS_DIR = os.environ['outputDataDir']

ENDC = '\033[m'
TGREEN_BOLD = '\033[32;1m'
TGREEN = '\033[32m'
TRED_BOLD = '\033[31;1m'
TWHITE = '\033[37m'
WHITE_BOLD = '\033[37;1m'
CYAN_BOLD = '\033[36;1m'


def profile_test_case_archive(archive_to_check, magnitude, stats_mode):
    """
    This function searches multiple directories and locates previously produced performance statistics.

    Args:
        archive_to_check (str): The directory path to search.
        magnitude (str): Because a benchmark dataset may have multiple magnitudes, this argument defines
                               which magnitude is to be used when searching for previous statistics.
    Returns:
        archive_dictionary (dict): A dictionary of available statistics for previous versions of the domain and magnitude.
                                  {version: {agreement_raster: agreement_raster_path, stats_csv: stats_csv_path, stats_json: stats_json_path}}
                                  *Will only add the paths to files that exist.

    """

    archive_dictionary = {}

    # List through previous version and check for available stats and maps. If available, add to dictionary.
    available_versions_list = os.listdir(archive_to_check)

    if len(available_versions_list) == 0:
        print("Cannot compare with -c flag because there are no data in the previous_versions directory.")
        return

    for version in available_versions_list:
        version_magnitude_dir = os.path.join(archive_to_check, version, magnitude)
        # Initialize dictionary for version and set paths to None by default.
        archive_dictionary.update({version: {'agreement_raster': None,
                                             'stats_csv': None,
                                             'stats_json': None}})
        # Find stats files and raster files and add to dictionary.
        agreement_raster = os.path.join(version_magnitude_dir, stats_mode + '_agreement.tif')
        stats_csv = os.path.join(version_magnitude_dir, stats_mode + '_stats.csv')
        stats_json = os.path.join(version_magnitude_dir, stats_mode + '_stats.json')

        if os.path.exists(agreement_raster):
            archive_dictionary[version]['agreement_raster'] = agreement_raster
        if os.path.exists(stats_csv):
            archive_dictionary[version]['stats_csv'] = stats_csv
        if os.path.exists(stats_json):
            archive_dictionary[version]['stats_json'] = stats_json

    return archive_dictionary


def compute_contingency_stats_from_rasters(predicted_raster_path, benchmark_raster_path, agreement_raster=None, stats_csv=None, stats_json=None, mask_values=None, stats_modes_list=['total_area'], test_id='', mask_dict={}):
    """
    This function contains FIM-specific logic to prepare raster datasets for use in the generic get_contingency_table_from_binary_rasters() function.
    This function also calls the generic compute_stats_from_contingency_table() function and writes the results to CSV and/or JSON, depending on user input.

    Args:
        predicted_raster_path (str): The path to the predicted, or modeled, FIM extent raster.
        benchmark_raster_path (str): The path to the benchmark, or truth, FIM extent raster.
        agreement_raster (str): Optional. An agreement raster will be written to this path. 0: True Negatives, 1: False Negative, 2: False Positive, 3: True Positive.
        stats_csv (str): Optional. Performance statistics will be written to this path. CSV allows for readability and other tabular processes.
        stats_json (str): Optional. Performance statistics will be written to this path. JSON allows for quick ingestion into Python dictionary in other processes.

    Returns:
        stats_dictionary (dict): A dictionary of statistics produced by compute_stats_from_contingency_table(). Statistic names are keys and statistic values are the values.
    """

    # Get cell size of benchmark raster.
    raster = rasterio.open(predicted_raster_path)
    t = raster.transform
    cell_x = t[0]
    cell_y = t[4]
    cell_area = abs(cell_x*cell_y)

    # Get contingency table from two rasters.
    contingency_table_dictionary = get_contingency_table_from_binary_rasters(benchmark_raster_path, predicted_raster_path, agreement_raster, mask_values=mask_values, mask_dict=mask_dict)

    stats_dictionary = {}

    for stats_mode in contingency_table_dictionary:
        true_negatives = contingency_table_dictionary[stats_mode]['true_negatives']
        false_negatives = contingency_table_dictionary[stats_mode]['false_negatives']
        false_positives = contingency_table_dictionary[stats_mode]['false_positives']
        true_positives = contingency_table_dictionary[stats_mode]['true_positives']
        masked_count = contingency_table_dictionary[stats_mode]['masked_count']
        file_handle = contingency_table_dictionary[stats_mode]['file_handle']

        # Produce statistics from continency table and assign to dictionary. cell_area argument optional (defaults to None).
        mode_stats_dictionary = compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area, masked_count)

        # Write the mode_stats_dictionary to the stats_csv.
        if stats_csv != None:
            stats_csv = os.path.join(os.path.split(stats_csv)[0], file_handle + '_stats.csv')
            df = pd.DataFrame.from_dict(mode_stats_dictionary, orient="index", columns=['value'])
            df.to_csv(stats_csv)

        # Write the mode_stats_dictionary to the stats_json.
        if stats_json != None:
            stats_json = os.path.join(os.path.split(stats_csv)[0], file_handle + '_stats.json')
            with open(stats_json, "w") as outfile:
                json.dump(mode_stats_dictionary, outfile)

        stats_dictionary.update({stats_mode: mode_stats_dictionary})

    return stats_dictionary


def check_for_regression(stats_json_to_test, previous_version, previous_version_stats_json_path, regression_test_csv=None):

    difference_dict = {}

    # Compare stats_csv to previous_version_stats_file
    stats_dict_to_test = json.load(open(stats_json_to_test))
    previous_version_stats_dict = json.load(open(previous_version_stats_json_path))

    for stat, value in stats_dict_to_test.items():
        previous_version_value = previous_version_stats_dict[stat]
        stat_value_diff = value - previous_version_value
        difference_dict.update({stat + '_diff': stat_value_diff})

    return difference_dict


def run_alpha_test(fim_run_dir, branch_name, test_id, magnitude, compare_to_previous=False, archive_results=False, mask_type='huc', inclusion_area='', inclusion_area_buffer=0, light_run=False):

    # Construct paths to development test results if not existent.
    if archive_results:
        branch_test_case_dir_parent = os.path.join(TEST_CASES_DIR, test_id, 'performance_archive', 'previous_versions', branch_name)
    else:
        branch_test_case_dir_parent = os.path.join(TEST_CASES_DIR, test_id, 'performance_archive', 'development_versions', branch_name)

    # Delete the entire directory if it already exists.
    if os.path.exists(branch_test_case_dir_parent):
        shutil.rmtree(branch_test_case_dir_parent)

    print("Running the alpha test for test_id: " + test_id + ", " + branch_name + "...")
    stats_modes_list = ['total_area']

    fim_run_parent = os.path.join(os.environ['outputDataDir'], fim_run_dir)
    assert os.path.exists(fim_run_parent), "Cannot locate " + fim_run_parent

    # Create paths to fim_run outputs for use in inundate().
    rem = os.path.join(fim_run_parent, 'rem_zeroed_masked.tif')
    catchments = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes.tif')
    if mask_type == 'huc':
        catchment_poly = ''
    else:
        catchment_poly = os.path.join(fim_run_parent, 'gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg')
    hydro_table = os.path.join(fim_run_parent, 'hydroTable.csv')
        
    # Map necessary inputs for inundation().
    hucs, hucs_layerName = os.path.join(INPUTS_DIR, 'wbd', 'WBD_National.gpkg'), 'WBDHU8'
    
    benchmark_category = test_id.split('_')[1]
    current_huc = test_id.split('_')[0]  # Break off HUC ID and assign to variable.

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
            
    if not os.path.exists(branch_test_case_dir_parent):
        os.mkdir(branch_test_case_dir_parent)
            
    # If the test_id is AHPS, then identify possible inclusion zones in the HUC.
    if benchmark_category == 'ahps':
        
        ahps_inclusion_zones_dir = os.path.join(branch_test_case_dir_parent, 'ahps_domains')
        print(ahps_inclusion_zones_dir)
        if not os.path.exists(ahps_inclusion_zones_dir):
            os.mkdir(ahps_inclusion_zones_dir)
        
        ahps_domain_shapefile = os.path.join(TEST_CASES_DIR, 'other', 'zones', 'ahps_domains.shp')
        
        # Open shapefile, determine the polys in the huc, create a different shapefile for each poly--name according to AHPS.
        # Open shapefile.
        ahps_domain_obj = gpd.read_file(ahps_domain_shapefile)
        ahps_domain_gdf = gpd.GeoDataFrame(ahps_domain_obj)
        
        
#        input_flows = gpd.read_file(input_flows_fileName)
#        # must drop leading zeroes
#        select_flows = tuple(map(str,map(int,wbd[wbd.HUC8.str.contains(hucCode)].fossid)))
#        
#        if input_flows.HydroID.dtype != 'str': input_flows.HydroID = input_flows.HydroID.astype(str)
#        output_flows = input_flows[input_flows.HydroID.str.startswith(select_flows)].copy()
#        if output_flows.HydroID.dtype != 'int': output_flows.HydroID = output_flows.HydroID.astype(int)

        
        # Loop through entries and compare against the huc4_list to get available HUCs within the geopackage domain.
        for index, row in ahps_domain_gdf.iterrows():
            print(row)
            huc8_code = row['huc8_code']
            ahps = row['ahps_code']
            
            if huc8_code == current_huc:
                print("YO")
                ahps_domain_subset = ahps_domain_obj[ahps_domain_obj.ahps_code == ahps]
                
                #.query("ahps_code=='{ahps_code}'".format(ahps_code=ahps_code))
                ahps_domain_subset_output = os.path.join(ahps_inclusion_zones_dir, ahps + '.shp')
                ahps_domain_subset.to_file(ahps_domain_subset_output,driver='ESRI Shapefile')

                
#                ahps_domain_gdf.to_file(ahps_domain_subset_output,driver='ESRI Shapefile')
                
                
#            huc = row["HUC" + huc_gpkg[-1]]
#            
#            # Append huc to appropriate list.
#            if str(huc[:4]) in huc4_list:
#                if huc_gpkg == 'WBDHU6':
#                    huc6_list.append(huc)
#                elif huc_gpkg == 'WBDHU8':
#                    huc8_list.append(huc)  
#        
#        
#        
#        
                
    if inclusion_area != '':
        inclusion_area_name = os.path.split(inclusion_area)[1].split('.')[0]  # Get layer name
        mask_dict.update({inclusion_area_name: {'path': inclusion_area,
                                                'buffer': int(inclusion_area_buffer),
                                                'operation': 'include'}})

    # Check if magnitude is list of magnitudes or single value.
    magnitude_list = magnitude
    if type(magnitude_list) != list:
        magnitude_list = [magnitude_list]

    for magnitude in magnitude_list:
        # Construct path to validation raster and forecast file.
        
        benchmark_raster_path = os.path.join(TEST_CASES_DIR, 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_depth_' + magnitude + '.tif')
        if not os.path.exists(benchmark_raster_path):  # Skip loop instance if the benchmark raster doesn't exist.
            continue

        branch_test_case_dir = os.path.join(branch_test_case_dir_parent, magnitude)

        os.makedirs(branch_test_case_dir)  # Make output directory for branch.

        # Define paths to inundation_raster and forecast file.
        inundation_raster = os.path.join(branch_test_case_dir, 'inundation_extent.tif')
        forecast = os.path.join(TEST_CASES_DIR, 'validation_data_' + benchmark_category, current_huc, magnitude, benchmark_category + '_huc_' + current_huc + '_flows_' + magnitude + '.csv')

        # Run inundate.
        print("-----> Running inundate() to produce modeled inundation extent for the " + magnitude + " magnitude...")
        inundate(
                 rem,catchments,catchment_poly,hydro_table,forecast,mask_type,hucs=hucs,hucs_layerName=hucs_layerName,
                 subset_hucs=current_huc,num_workers=1,aggregate=False,inundation_raster=inundation_raster,inundation_polygon=None,
                 depths=None,out_raster_profile=None,out_vector_profile=None,quiet=True
                )

        print("-----> Inundation mapping complete.")
        predicted_raster_path = os.path.join(os.path.split(inundation_raster)[0], os.path.split(inundation_raster)[1].replace('.tif', '_' + current_huc + '.tif'))  # The inundate adds the huc to the name so I account for that here.

        # Define outputs for agreement_raster, stats_json, and stats_csv.

        agreement_raster, stats_json, stats_csv = os.path.join(branch_test_case_dir, 'total_area_agreement.tif'), os.path.join(branch_test_case_dir, 'stats.json'), os.path.join(branch_test_case_dir, 'stats.csv')

        test_version_dictionary = compute_contingency_stats_from_rasters(predicted_raster_path,
                                                                         benchmark_raster_path,
                                                                         agreement_raster,
                                                                         stats_csv=stats_csv,
                                                                         stats_json=stats_json,
                                                                         mask_values=[],
                                                                         stats_modes_list=stats_modes_list,
                                                                         test_id=test_id,
                                                                         mask_dict=mask_dict,
                                                                         )
        print(" ")
        print("Evaluation complete. All metrics for " + test_id + ", " + branch_name + ", " + magnitude + " are available at " + CYAN_BOLD + branch_test_case_dir + ENDC)
        print(" ")

        if compare_to_previous:
            text_block = []
            # Compare to previous stats files that are available.
            archive_to_check = os.path.join(TEST_CASES_DIR, test_id, 'performance_archive', 'previous_versions')
            for stats_mode in stats_modes_list:
                archive_dictionary = profile_test_case_archive(archive_to_check, magnitude, stats_mode)

                if archive_dictionary == {}:
                    break

                # Create header for section.
                header = [stats_mode]
                for previous_version, paths in archive_dictionary.items():
                    header.append(previous_version)
                header.append(branch_name)
                text_block.append(header)

                # Loop through stats in PRINTWORTHY_STATS for left.
                for stat in PRINTWORTHY_STATS:
                    stat_line = [stat]
                    for previous_version, paths in archive_dictionary.items():
                        # Load stats for previous version.
                        previous_version_stats_json_path = paths['stats_json']
                        previous_version_stats_dict = json.load(open(previous_version_stats_json_path))

                        # Append stat for the version to state_line.
                        stat_line.append(previous_version_stats_dict[stat])

                    # Append stat for the current version to stat_line.
                    stat_line.append(test_version_dictionary[stats_mode][stat])

                    text_block.append(stat_line)

                text_block.append([" "])

            regression_report_csv = os.path.join(branch_test_case_dir, 'stats_summary.csv')
            with open(regression_report_csv, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerows(text_block)

            print()
            print("--------------------------------------------------------------------------------------------------")
            
            stats_mode = stats_modes_list[0]
            try:
                last_version_index = text_block[0].index('dev_latest')
            except ValueError:
                try:
                    last_version_index = text_block[0].index('fim_2_3_3')
                except ValueError:
                    try:
                        last_version_index = text_block[0].index('fim_1_0_0')
                    except ValueError:
                        print(TRED_BOLD + "Warning: " + ENDC + "Cannot compare " + branch_name + " to a previous version because no authoritative versions were found in previous_versions directory. Future version of run_test_case may allow for comparisons between dev branches.")
                        print()
                        continue

            current_version_index = text_block[0].index(branch_name)

            for line in text_block:
                first_item = line[0]
                if first_item in stats_modes_list:
                    if first_item != stats_mode:  # Update the stats_mode and print a separator.
                        print()
                        print()
                        print("--------------------------------------------------------------------------------------------------")
                    print()
                    stats_mode = first_item
                    print(CYAN_BOLD + current_huc + ": " + magnitude.upper(), ENDC)
                    print(CYAN_BOLD + stats_mode.upper().replace('_', ' ') + " METRICS" + ENDC)
                    print()

                    color = WHITE_BOLD
                    metric_name = '      '.center(len(max(PRINTWORTHY_STATS, key=len)))
                    percent_change_header = '% CHG'
                    difference_header = 'DIFF'
                    current_version_header = line[current_version_index].upper()
                    last_version_header = line[last_version_index].upper()
                    # Print Header.
                    print(color + metric_name + "      " + percent_change_header.center((7)) + "       " + difference_header.center((15))  + "    " + current_version_header.center(18) + " " + last_version_header.center(18), ENDC)
                # Format and print stat row.
                elif first_item in PRINTWORTHY_STATS:
                    stat_name = first_item.upper().center(len(max(PRINTWORTHY_STATS, key=len))).replace('_', ' ')
                    current_version = round((line[current_version_index]), 3)
                    last_version = round((line[last_version_index]) + 0.000, 3)
                    difference = round(current_version - last_version, 3)
                    if difference > 0:
                        symbol = '+'
                        if first_item in GO_UP_STATS:
                            color = TGREEN_BOLD
                        elif first_item in GO_DOWN_STATS:
                            color = TRED_BOLD
                        else:
                            color = TWHITE
                    if difference < 0:
                        symbol = '-'
                        if first_item in GO_UP_STATS:
                            color = TRED_BOLD
                        elif first_item in GO_DOWN_STATS:
                            color = TGREEN_BOLD
                        else:
                            color = TWHITE

                    if difference == 0 :
                        symbol, color = '+', TGREEN
                    percent_change = round((difference / last_version)*100,2)

                    print(WHITE_BOLD + stat_name + ENDC + "     " + color + (symbol + " {:5.2f}".format(abs(percent_change)) + " %").rjust(len(percent_change_header)), ENDC + "    " + color + ("{:12.3f}".format((difference))).rjust(len(difference_header)), ENDC + "    " + "{:15.3f}".format(current_version).rjust(len(current_version_header)) + "   " + "{:15.3f}".format(last_version).rjust(len(last_version_header)) + "  ")

            print()
            print()
            print()
            print("--------------------------------------------------------------------------------------------------")
            print()


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Inundation mapping and regression analysis for FOSS FIM. Regression analysis results are stored in the test directory.')
    parser.add_argument('-r','--fim-run-dir',help='Name of directory containing outputs of fim_run.sh',required=True)
    parser.add_argument('-b', '--branch-name',help='The name of the working branch in which features are being tested',required=True,default="")
    parser.add_argument('-t', '--test-id',help='The test_id to use. Format as: HUC_BENCHMARKTYPE, e.g. 12345678_ble.',required=True,default="")
    parser.add_argument('-m', '--mask-type', help='Specify \'huc\' (FIM < 3) or \'filter\' (FIM >= 3) masking method', required=False,default="huc")
    parser.add_argument('-y', '--magnitude',help='The magnitude to run.',required=False, default="")
    parser.add_argument('-c', '--compare-to-previous', help='Compare to previous versions of HAND.', required=False,action='store_true')
    parser.add_argument('-a', '--archive-results', help='Automatically copy results to the "previous_version" archive for test_id. For admin use only.', required=False,action='store_true')
    parser.add_argument('-i', '--inclusion-area', help='Path to shapefile. Contingency metrics will be produced from pixels inside of shapefile extent.', required=False, default="")
    parser.add_argument('-ib','--inclusion-area-buffer', help='Buffer to use when masking contingency metrics with inclusion area.', required=False, default="0")
    parser.add_argument('-l', '--light-run', help='Using the light_run option will result in only stat files being written, and NOT grid files.', required=False, action='store_true')

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
            args['magnitude'] = ['action', 'minor']
#                args['magnitude'] = ['action', 'minor', 'moderate', 'major']
        else:
            print(TRED_BOLD + "Error: " + WHITE_BOLD + "The provided magnitude (-y) " + CYAN_BOLD + args['magnitude'] + WHITE_BOLD + " is invalid. ble options include: 100yr, 500yr. ahps options include action, minor, moderate, major." + ENDC)
            exit_flag = True     
            
            
    if exit_flag:
        print()
        sys.exit()
        

    else:

        run_alpha_test(**args)
