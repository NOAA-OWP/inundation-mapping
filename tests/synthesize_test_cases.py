#!/usr/bin/env python3

import os
import argparse
from multiprocessing import Pool
import json
import csv

from run_test_case import run_alpha_test
from utils.shared_variables import TEST_CASES_DIR, PREVIOUS_FIM_DIR, OUTPUTS_DIR


def create_master_metrics_csv(master_metrics_csv_output):
            
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
    list_to_write = [additional_header_info_prefix + metrics_to_write + ['full_json_path'] + ['flow'] + ['benchmark_source'] + ['extent_config']]
    
    versions_to_aggregate = os.listdir(PREVIOUS_FIM_DIR)
    
    for benchmark_source in ['ble', 'nws', 'usgs']:
        
        benchmark_test_case_dir = os.path.join(TEST_CASES_DIR, benchmark_source + '_test_cases')
        
        if benchmark_source == 'ble':
            test_cases_list = os.listdir(benchmark_test_case_dir)
                            
            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])
                    
                    huc = test_case.split('_')[0]
                    official_versions = os.path.join(benchmark_test_case_dir, test_case, 'official_versions')
                    
                    for magnitude in ['100yr', '500yr']:
                        for version in versions_to_aggregate:
                            if '_fr_' in version:
                                extent_config = 'FR'
                            if '_ms_' in version:
                                extent_config = 'MS'
                            if '_fr_' or '_ms_' not in version:
                                extent_config = 'FR'
                            version_dir = os.path.join(official_versions, version)
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
                                            sub_list_to_append.append(extent_config)
                                            
                                            list_to_write.append(sub_list_to_append)
                except ValueError:
                    pass
                
        if benchmark_source in ['nws', 'usgs']:
            test_cases_list = os.listdir(TEST_CASES_DIR)

            for test_case in test_cases_list:
                try:
                    int(test_case.split('_')[0])
                    
                    huc = test_case.split('_')[0]
                    official_versions = os.path.join(benchmark_test_case_dir, test_case, 'performance_archive', 'previous_versions')
                    
                    for magnitude in ['action', 'minor', 'moderate', 'major']:
                        for version in versions_to_aggregate:
                            if '_fr_' in version:
                                extent_config = 'FR'
                            if '_ms_' in version:
                                extent_config = 'MS'
                            if '_fr_' or '_ms_' not in version:
                                extent_config = 'FR'
                                
                            version_dir = os.path.join(official_versions, version)
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
                                            flow_file = os.path.join(benchmark_test_case_dir, 'validation_data_' + benchmark_source, huc, nws_lid, magnitude, 'ahps_' + nws_lid + '_huc_' + huc + '_flows_' + magnitude + '.csv')
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
                                            sub_list_to_append.append(extent_config)
                                            
                                            list_to_write.append(sub_list_to_append)
                except ValueError:
                    pass
        
    with open(master_metrics_csv_output, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerows(list_to_write)


def process_alpha_test(args):
    
    fim_run_dir = args[0]
    version = args[1]
    test_id = args[2]
    magnitude = args[3]
    archive_results = args[4]
    overwrite = args[5]
    
    mask_type = 'huc'    
    
    if archive_results == False:
        compare_to_previous = True
    else:
        compare_to_previous = False

    try:
        run_alpha_test(fim_run_dir, version, test_id, magnitude, compare_to_previous=compare_to_previous, archive_results=archive_results, mask_type=mask_type, overwrite=overwrite)
    except Exception as e:
        print(e)


if __name__ == '__main__':

    # Parse arguments.
    parser = argparse.ArgumentParser(description='Caches metrics from previous versions of HAND.')
    parser.add_argument('-c','--config',help='Save outputs to development_versions or previous_versions? Options: "DEV" or "PREV"',required=True)
    parser.add_argument('-v','--fim-version',help='Name of fim version to cache.',required=False, default="all")
    parser.add_argument('-j','--job-number',help='Number of processes to use. Default is 1.',required=False, default="1")
    parser.add_argument('-s','--special-string',help='Add a special name to the end of the branch.',required=False, default="")
    parser.add_argument('-b','--benchmark-category',help='A benchmark category to specify. Defaults to process all categories.',required=False, default="all")
    parser.add_argument('-o','--overwrite',help='Overwrite all metrics or only fill in missing metrics.',required=False, action="store_true")
    parser.add_argument('-m','--master-metrics-csv',help='Define path for master metrics CSV file.',required=True)
        
    # Assign variables from arguments.
    args = vars(parser.parse_args())
    config = args['config']
    fim_version = args['fim_version']
    job_number = int(args['job_number'])
    special_string = args['special_string']
    benchmark_category = args['benchmark_category']
    overwrite = args['overwrite']
    master_metrics_csv = args['master_metrics_csv']
        
    # Default to processing all possible versions in PREVIOUS_FIM_DIR. Otherwise, process only the user-supplied version.
    if fim_version != "all":
        previous_fim_list = [fim_version]
    else:
        if config == 'PREV':
            previous_fim_list = os.listdir(PREVIOUS_FIM_DIR)
        elif config == 'DEV':
            previous_fim_list = os.listdir(OUTPUTS_DIR)
    
    # Define whether or not to archive metrics in "official_versions" or "testing_versions" for each test_id.
    if config == 'PREV':
        archive_results = True
    elif config == 'DEV':
        archive_results = False
    else:
        print('Config (-c) option incorrectly set. Use "DEV" or "PREV"')
     
    # List all available benchmark categories and test_cases.
    test_cases_dir_list = os.listdir(TEST_CASES_DIR)
    benchmark_category_list = []
    if benchmark_category == "all":
        for d in test_cases_dir_list:
            if 'test_cases' in d:
                benchmark_category_list.append(d.replace('_test_cases', ''))
    else:
        benchmark_category_list = [benchmark_category]
        
    # Loop through benchmark categories.
    procs_list = []
    for bench_cat in benchmark_category_list:
        
        # Map path to appropriate test_cases folder and list test_ids into bench_cat_id_list.
        bench_cat_test_case_dir = os.path.join(TEST_CASES_DIR, bench_cat + '_test_cases')
        bench_cat_id_list = os.listdir(bench_cat_test_case_dir)
    
        # Loop through test_ids in bench_cat_id_list.
        for test_id in bench_cat_id_list:
            if 'validation' and 'other' not in test_id:
                current_huc = test_id.split('_')[0]
                if test_id.split('_')[1] in bench_cat:
                
                    # Loop through versions.
                    for version in previous_fim_list:
                        if config == 'DEV':
                            fim_run_dir = os.path.join(OUTPUTS_DIR, version, current_huc)
                        elif config == 'PREV':
                            fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc)
                                                
                        # For previous versions of HAND computed at HUC6 scale
                        if not os.path.exists(fim_run_dir):
                            if config == 'DEV':
                                fim_run_dir = os.path.join(OUTPUTS_DIR, version, current_huc[:6])
                                print(fim_run_dir)
                            elif config == 'PREV':
                                fim_run_dir = os.path.join(PREVIOUS_FIM_DIR, version, current_huc[:6])  
                        
                        if os.path.exists(fim_run_dir):
                            
                            # If a user supplies a specia_string (-s), then add it to the end of the created dirs.
                            if special_string != "":
                                version = version + '_' + special_string
                            
                            # Define the magnitude lists to use, depending on test_id.
                            if 'ble' in test_id:
                                magnitude = ['100yr', '500yr']
                            elif 'usgs' or 'nws' in test_id:
                                magnitude = ['action', 'minor', 'moderate', 'major']
                            else:
                                continue
                        
                            # Either add to list to multiprocess or process serially, depending on user specification.
                            if job_number > 1:
                                procs_list.append([fim_run_dir, version, test_id, magnitude, archive_results, overwrite])
                            else:                            
                                process_alpha_test([fim_run_dir, version, test_id, magnitude, archive_results, overwrite])

    # Multiprocess alpha test runs.
    if job_number > 1:
        pool = Pool(job_number)
        pool.map(process_alpha_test, procs_list)
    
    # Do aggregate_metrics.
    print("Creating master metrics CSV...")
    create_master_metrics_csv(master_metrics_csv_output=master_metrics_csv)
    