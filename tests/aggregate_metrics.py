#!/usr/bin/env python3

import json
import os
import csv
    
import argparse

TEST_CASES_DIR = r'/data/test_cases_new/'
# TEMP = r'/data/temp'

# Search through all previous_versions in test_cases
from utils.shared_functions import compute_stats_from_contingency_table

def create_master_metrics_csv():
            
    # Construct header
    metrics_to_write = ['true_negatives_count',
                        'false_negatives_count',
                        'true_positives_count',
                        'false_positives_count',
                        'contingency_tot_count',
                        'cell_area_m2',
                        'TP_area_km2',
                        'FP_area_km2',
                        'TN_area_km2',
                        'FN_area_km2',
                        'contingency_tot_area_km2',
                        'predPositive_area_km2',
                        'predNegative_area_km2',
                        'obsPositive_area_km2',
                        'obsNegative_area_km2',
                        'positiveDiff_area_km2',
                        'CSI',
                        'FAR',
                        'TPR',
                        'TNR',
                        'PPV',
                        'NPV',
                        'ACC',
                        'Bal_ACC',
                        'MCC',
                        'EQUITABLE_THREAT_SCORE',
                        'PREVALENCE',
                        'BIAS',
                        'F1_SCORE',
                        'TP_perc',
                        'FP_perc',
                        'TN_perc',
                        'FN_perc',
                        'predPositive_perc',
                        'predNegative_perc',
                        'obsPositive_perc',
                        'obsNegative_perc',
                        'positiveDiff_perc',
                        'masked_count',
                        'masked_perc',
                        'masked_area_km2'
                        ]
    
    additional_header_info_prefix = ['version', 'nws_lid', 'magnitude', 'huc']
    list_to_write = [additional_header_info_prefix + metrics_to_write + ['full_json_path'] + ['flow'] + ['benchmark_source']]
    
    for benchmark_type in ['ble', 'ahps']:
        
        if benchmark_type == 'ble':
        
            test_cases = r'/data/test_cases'
            test_cases_list = os.listdir(test_cases)
            # AHPS test_ids
            versions_to_aggregate = ['fim_1_0_0', 'fim_2_3_3', 'fim_3_0_0_3_fr_c']
                            
            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])
                    
                    huc = test_case.split('_')[0]
                    previous_versions = os.path.join(test_cases, test_case, 'performance_archive', 'previous_versions')
                    
                    for magnitude in ['100yr', '500yr']:
                        for version in versions_to_aggregate:
                            version_dir = os.path.join(previous_versions, version)
                            magnitude_dir = os.path.join(version_dir, magnitude)

                            if os.path.exists(magnitude_dir):
                                
                                magnitude_dir_list = os.listdir(magnitude_dir)
                                for f in magnitude_dir_list:
                                    if '.json' in f:
                                        flow = 'NA'
                                        nws_lid = "NA"
                                        benchmark_source = 'ble'
                                        sub_list_to_append = [version, nws_lid, magnitude, huc]
                                        full_json_path = os.path.join(magnitude_dir, f)
                                        if os.path.exists(full_json_path):
                                            stats_dict = json.load(open(full_json_path))
                                            for metric in metrics_to_write:
                                                sub_list_to_append.append(stats_dict[metric])
                                            sub_list_to_append.append(full_json_path)
                                            sub_list_to_append.append(flow)
                                            sub_list_to_append.append(benchmark_source)
                                            
                                            list_to_write.append(sub_list_to_append)
                                                                                    
                except ValueError:
                    pass
                
        if benchmark_type == 'ahps':
    
            test_cases = r'/data/test_cases_ahps_testing'
            test_cases_list = os.listdir(test_cases)
            # AHPS test_ids
            versions_to_aggregate = ['fim_1_0_0_nws_1_21_2021', 'fim_1_0_0_usgs_1_21_2021', 
                                     'fim_2_x_ms_nws_1_21_2021', 'fim_2_x_ms_usgs_1_21_2021',
                                     'fim_3_0_0_3_ms_c_nws_1_21_2021', 'fim_3_0_0_3_ms_c_usgs_1_21_2021',
                                     'ms_xwalk_fill_missing_cal_nws', 'ms_xwalk_fill_missing_cal_usgs']
            
            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])
                    
                    huc = test_case.split('_')[0]
                    previous_versions = os.path.join(test_cases, test_case, 'performance_archive', 'previous_versions')
                    
                    for magnitude in ['action', 'minor', 'moderate', 'major']:
                        for version in versions_to_aggregate:
                            
                            if 'nws' in version:
                                benchmark_source = 'ahps_nws'
                            if 'usgs' in version:
                                benchmark_source = 'ahps_usgs'
                            
                            version_dir = os.path.join(previous_versions, version)
                            magnitude_dir = os.path.join(version_dir, magnitude)
                            
                            if os.path.exists(magnitude_dir):
                                magnitude_dir_list = os.listdir(magnitude_dir)
                                for f in magnitude_dir_list:
                                    if '.json' in f and 'total_area' not in f:
                                        nws_lid = f[:5]
                                        sub_list_to_append = [version, nws_lid, magnitude, huc]
                                        full_json_path = os.path.join(magnitude_dir, f)
                                        flow = ''
                                        if os.path.exists(full_json_path):
                                            # Get flow used to map.
                                            if 'usgs' in version:
                                                parent_dir = 'usgs_1_21_2021'
                                            if 'nws' in version:
                                                parent_dir = 'nws_1_21_2021'
                                                
                                            flow_file = os.path.join(test_cases, parent_dir, huc, nws_lid, magnitude, 'ahps_' + nws_lid + '_huc_' + huc + '_flows_' + magnitude + '.csv')
                                            if os.path.exists(flow_file):
                                                with open(flow_file, newline='') as csv_file:
                                                    reader = csv.reader(csv_file)
                                                    next(reader)
                                                    for row in reader:
                                                        flow = row[1]
                                                    if nws_lid == 'mcc01':
                                                        print(flow)
                                            
                                            stats_dict = json.load(open(full_json_path))
                                            for metric in metrics_to_write:
                                                sub_list_to_append.append(stats_dict[metric])
                                            sub_list_to_append.append(full_json_path)
                                            sub_list_to_append.append(flow)
                                            sub_list_to_append.append(benchmark_source)
                                            list_to_write.append(sub_list_to_append)
                                        
                except ValueError:
                    pass
        
    with open(output_csv, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)



def aggregate_metrics(config="DEV", branch="", hucs="", special_string="", outfolder=""):

    # Read hucs into list.
    if hucs != "":
        huc_list = [line.rstrip('\n') for line in open(hucs)]

    else:
        huc_list = None

    if config == "DEV":
        config_version = "development_versions"
    elif config == "PREV":
        config_version = "previous_versions"

    # Make directory to store output aggregates.
    if special_string != "":
        special_string = "_" + special_string
    aggregate_output_dir = os.path.join(outfolder, 'aggregate_metrics', branch + '_aggregate_metrics' + special_string)
    if not os.path.exists(aggregate_output_dir):
        os.makedirs(aggregate_output_dir)

    test_cases_dir_list = os.listdir(TEST_CASES_DIR)

    for magnitude in ['100yr', '500yr', 'action', 'minor', 'moderate', 'major']:
        huc_path_list = [['huc', 'path']]
        true_positives, true_negatives, false_positives, false_negatives, cell_area, masked_count = 0, 0, 0, 0, 0, 0
        
        for test_case in test_cases_dir_list:

            if test_case not in ['other', 'validation_data_ble', 'validation_data_legacy', 'validation_data_ahps']:
                branch_results_dir = os.path.join(TEST_CASES_DIR, test_case, 'performance_archive', config_version, branch)

                huc = test_case.split('_')[0]
                # Check that the huc is in the list of hucs to aggregate.
                if huc_list != None and huc not in huc_list:
                    continue

                stats_json_path = os.path.join(branch_results_dir, magnitude, 'total_area_stats.json')

                # If there is a stats json for the test case and branch name, use it when aggregating stats.
                if os.path.exists(stats_json_path):
                    json_dict = json.load(open(stats_json_path))

                    true_positives += json_dict['true_positives_count']
                    true_negatives += json_dict['true_negatives_count']
                    false_positives += json_dict['false_positives_count']
                    false_negatives += json_dict['false_negatives_count']
                    masked_count += json_dict['masked_count']

                    cell_area = json_dict['cell_area_m2']

                    huc_path_list.append([huc, stats_json_path])
                
                    
            if cell_area == 0:
                continue
            
            # Pass all sums to shared function to calculate metrics.
            stats_dict = compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area=cell_area, masked_count=masked_count)

            list_to_write = [['metric', 'value']]  # Initialize header.

            for stat in stats_dict:
                list_to_write.append([stat, stats_dict[stat]])
                
            # Map path to output directory for aggregate metrics.
            output_file = os.path.join(aggregate_output_dir, branch + '_aggregate_metrics_' + magnitude + special_string + '.csv')

        if cell_area != 0:
            with open(output_file, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerows(list_to_write)
                csv_writer.writerow([])
                csv_writer.writerows(huc_path_list)
    
            print()
            print("Finished aggregating for the '" + magnitude + "' magnitude. Aggregated metrics over " + str(len(huc_path_list)-1) + " test cases.")
            print()
            print("Results are at: " + output_file)
            print()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregates a metric or metrics for multiple HUC8s.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=False)
    parser.add_argument('-b','--branch',help='Name of branch to check all test_cases for and to aggregate.',required=True)
    parser.add_argument('-u','--hucs',help='HUC8s to restrict the aggregation.',required=False, default="")
    parser.add_argument('-s','--special_string',help='Special string to add to outputs.',required=False, default="")
    parser.add_argument('-f','--outfolder',help='output folder',required=True,type=str)

    args = vars(parser.parse_args())

    aggregate_metrics(**args)
