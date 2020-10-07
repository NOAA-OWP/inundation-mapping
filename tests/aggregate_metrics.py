import json
import os
import csv

import argparse

TEST_CASES_DIR = r'/data/test_cases/'
TEMP = r'/data/temp'

from utils.shared_functions import compute_stats_from_contingency_table


def aggregate_metrics(config="DEV", branch_name="", hucs="", special_string=""):
    
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
    aggregate_output_dir = os.path.join(TEMP, 'aggregate_metrics', branch_name + '_aggregate_metrics' + special_string)
    if not os.path.exists(aggregate_output_dir):
        os.mkdir(aggregate_output_dir)
    
    test_cases_dir_list = os.listdir(TEST_CASES_DIR)
            
    true_positives, true_negatives, false_positives, false_negatives, cell_area, masked_count = 0, 0, 0, 0, 0, 0
    
    for return_interval in ['100yr', '500yr']:
        huc_path_list = [['huc', 'path']]
        for test_case in test_cases_dir_list:
            
            if test_case not in ['other', 'validation_data_ble', 'validation_data_legacy']:
                branch_results_dir = os.path.join(TEST_CASES_DIR, test_case, 'performance_archive', config_version, branch_name)
                
                huc = test_case.split('_')[0]
                if huc_list != None and huc not in huc_list:
                    continue
                
                stats_json_path = os.path.join(branch_results_dir, return_interval, 'total_area_stats.json')
                     
                if os.path.exists(stats_json_path):
                    json_dict = json.load(open(stats_json_path))
                                
                    true_positives += json_dict['true_positives_count']
                    true_negatives += json_dict['true_negatives_count']
                    false_positives += json_dict['false_positives_count']
                    false_negatives += json_dict['false_negatives_count']
                    masked_count += json_dict['masked_count']
                    
                    cell_area = json_dict['cell_area_m2']
                    
                    huc_path_list.append([huc, stats_json_path])
            
        stats_dict = compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area=cell_area, masked_count=masked_count)
        
        list_to_write = [['metric', 'value']]
        
        for stat in stats_dict:
            list_to_write.append([stat, stats_dict[stat]])
            
        output_file = os.path.join(aggregate_output_dir, branch_name + '_aggregate_metrics_' + return_interval + special_string + '.csv')
            
        with open(output_file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerows(list_to_write)
            csv_writer.writerow([])
            csv_writer.writerows(huc_path_list)
            
        print()
        print("Finished aggregating for " + return_interval + ". Aggregated metrics over " + str(len(huc_path_list)) + " test cases.")
        print()
        print("Results are at: " + output_file)
        print()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Aggregates a metric or metrics for multiple HUC8s.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    parser.add_argument('-b','--branch-name',help='Name of branch to check all test_cases for and to aggregate.',required=True)
    parser.add_argument('-u','--hucs',help='HUC8s to restrict the aggregation.',required=False, default="")
    parser.add_argument('-s','--special_string',help='Special string to add to outputs.',required=False, default="")
    
    args = vars(parser.parse_args())
    
    aggregate_metrics(**args)
    
                